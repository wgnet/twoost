# coding: utf-8

from __future__ import print_function, division

"""
Simple Twisted-style abstraction around AMQP pika library.
"""

import json
import uuid
import functools

try:
    import msgpack
except ImportError:
    msgpack = None

if not msgpack:
    try:
        import umsgpack as msgpack
    except ImportError:
        pass

import zope.interface

from twisted.internet import defer, reactor
from twisted.internet.error import ConnectionDone
from twisted.python import failure, components, reflect
from twisted.application import service

from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.spec import BasicProperties as _BasicProperties
from pika.connection import ConnectionParameters as _ConnectionParameters
from pika.credentials import PlainCredentials as _PlainCredentials
from pika.exceptions import MethodNotImplemented, ChannelClosed

from twoost import timed, pclient


import logging
logger = logging.getLogger(__name__)


__all__ = [
    'AMQPMessage',
    'AMQPService',
    'IAMQPSchema',
    'IAMQPSchemaBuilder'
]


# ---

class IAMQPSchema(zope.interface.Interface):

    def declareSchema(schemaBuidrer):
        pass


class IAMQPSchemaBuilder(zope.interface.Interface):

    def declareQueue(
            queue,
            dead_letter_exchange=None,
            dead_letter_exchange_rk=None,
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=False,
            message_ttl=None,
            arguments=None,
    ):
        pass

    def declareExchange(
            exchange,
            exchange_type='direct',
            passive=False,
            durable=False,
            auto_delete=False,
            internal=False,
            arguments=None,
    ):
        pass

    def bindQueue(
            exchange,
            queue,
            routing_key='',
            arguments=None,
    ):
        pass

    def bindExchange(
            source,
            destination,
            routing_key='',
            arguments=None,
    ):
        pass


# ---

class AMQPMessage(object):

    def __init__(self, body, deliver, properties):
        self.body = body
        self.deliver = deliver
        self.properties = properties

    @property
    def data(self):
        self.data = deserialize(self.body, self.content_type)
        return self.data

    def __getattr__(self, name):
        try:
            return getattr(self.properties, name)
        except AttributeError:
            return getattr(self.deliver, name)

    def __repr__(self):
        return ("<AMQPMessage(exchange={s.exchange!r},"
                "routing_key={s.routing_key!r},"
                "body={s.body!r}>".format(s=self))

    def __dir__(self):
        r = list(set(self.properties.__dict__.keys()
                     + self.deliver.__dict__.keys()
                     + self.__dict__.keys()))
        r.sort()
        return r


class _NopeSerializer(object):

    @staticmethod
    def loads(s):
        return s

    @staticmethod
    def dumps(s):
        return s


MESSAGE_SERIALIZERS = {
    None: _NopeSerializer,
    'plain/text': _NopeSerializer,
    'application/octet-stream': _NopeSerializer,
    'json': json,
    'application/json': json,
}


if msgpack:
    MESSAGE_SERIALIZERS.update({
        'msgpack': msgpack,
        'application/x-msgpack': msgpack,
        'application/msgpack': msgpack,
    })


def deserialize(data, content_type):
    logger.debug("deserialize data %r, content_type %r", data, content_type)
    if not content_type:
        return data
    s = MESSAGE_SERIALIZERS[content_type.lower()]
    return s.loads(data)


def serialize(data, content_type):
    logger.debug("serialize data %r, content_type %r", data, content_type)
    if not content_type:
        return data
    s = MESSAGE_SERIALIZERS[content_type.lower()]
    return s.dumps(data)


# ---

class _PikaQueueUnconsumed(Exception):
    pass


class _NotReadyForPublish(Exception):
    pass


class _SchemaBuilderProxy(components.proxyForInterface(IAMQPSchemaBuilder)):
    pass


class _AMQPProtocol(TwistedProtocolConnection, pclient.PersistentClientProtocol):

    ON_ERROR_STRATEGIES = (
        'requeue_once',          # requeue reject only redelivered messages
        'requeue_forever',  # requeue *all* messages to same queue inf times
        'reject',           # reject all messages (with delay `requeue_delay`)
        'requeue_hold',     # hold only *redelivered* messages until reconnect
        'do_nothing',       # do nothing - hold *all* messages until reconnect
    )
    __consumer_tag_cnt = 0

    def __init__(
            self,
            parameters,
            schema=None,
            on_error='requeue',
            prefetch_count=None,
            requeue_delay=None,
            requeue_max_count=None,
            **kwargs
    ):

        logger.debug("construct new _AMQPProtocol (id = %r)...", id(self))
        TwistedProtocolConnection.__init__(
            self, _ConnectionParameters(**parameters))

        self.virtual_host = parameters.get('virtual_host') or ""
        self.clock = reactor
        self.schema = schema
        self.prefetch_count = prefetch_count

        assert not on_error or on_error in self.ON_ERROR_STRATEGIES
        self.on_error = on_error or 'requeue'
        self.requeue_max_count = requeue_max_count if requeue_max_count is not None else 50000
        self.requeue_delay = requeue_delay if requeue_delay is not None else 120

        # -- state
        self._published_messages = {}
        self._consumer_state = {}
        self._delayed_requeue_tasks = {}
        self._ready_for_publish = False

    def connectionMade(self):
        logger.debug("amqp connection was made")
        TwistedProtocolConnection.connectionMade(self)
        self.ready.addCallback(lambda _: self.handshakingMade())
        self.ready.addErrback(self.handshakingFailed)

    def handshakingFailed(self, f):
        logger.error("handshaking with %r failed - disconnect due to %s", self.virtual_host, f)
        self.transport.loseConnection()
        self.protocolFailed(failure)

    @defer.inlineCallbacks
    def _open_write_channel(self):
        self._write_channel = yield self.channel()
        self._write_channel.add_on_close_callback(self._on_write_channel_closed)
        logger.debug("open channel (write) %r " % self._write_channel)

    @defer.inlineCallbacks
    def _open_safewrite_channel(self):
        try:
            self._safewrite_channel = yield self.channel()
            self._publish_delivery_tag_counter = 0
            yield self._safewrite_channel.confirm_delivery(callback=self._onPublishConfirm)
        except MethodNotImplemented:
            logger.warning("server doesn't support 'confirm delivery'")
            self._safewrite_channel = None
        else:
            self._safewrite_channel.add_on_close_callback(self._on_safewrite_channel_closed)
            logger.debug("open channel (safe write) %r" % self._safewrite_channel)

    @defer.inlineCallbacks
    def handshakingMade(self):
        logger.info("handshaking with %r was made", self.virtual_host)

        yield self._open_write_channel()
        yield self._open_safewrite_channel()

        if self.schema:
            logger.debug("declare schema...")
            yield defer.maybeDeferred(
                IAMQPSchema(self.schema).declareSchema,
                _SchemaBuilderProxy(self))
            logger.debug("amqp schema has been declared")

        self._ready_for_publish = True
        self.protocolReady()

    def _on_consuming_channel_closed(self, ct, channel, reply_code, reply_text):
        logger.error(
            "server closed channel, ct %r, reply_code %s, reply_text %s!",
            ct, reply_code, reply_text)
        s = self._consumer_state.pop(ct, None)
        if not s:
            return
        logger.debug("reconsume queue, prev state is %r", s)
        self.consumeQueue(
            queue=s['queue'],
            callback=s['callback'],
            no_ack=s['no_ack'],
            consumer_tag=ct,
            parallel=s['parallel'],
            **s.get('kwargs', {})
        )

    def _on_write_channel_closed(self, channel, reply_code, reply_text):
        logger.error(
            "server closed write channel, reply_code %s, reply_text %s!",
            reply_code, reply_text)
        # just print warn & reopen channel
        return self._open_write_channel()

    @defer.inlineCallbacks
    def _on_safewrite_channel_closed(self, channel, reply_code, reply_text):
        yield self._open_safewrite_channel()
        self._fail_published_messages(ChannelClosed(reply_code, reply_text))

    def _onPublishConfirm(self, a):

        delivery_tag = a.method.delivery_tag
        method_name = type(a.method).__name__
        ack = method_name == 'Ack'
        deferred_method = (defer.Deferred.callback if ack else defer.Deferred.errback)

        if a.method.multiple:
            logger.debug("multiple confirm - method %r, delivery_tag %d ...",
                         method_name, delivery_tag)
            for k in list(self._published_messages):
                if k <= delivery_tag:
                    logger.debug("confirm - method %r, delivery_tag %d", method_name, k)
                    d = self._published_messages.pop(k)
                    deferred_method(d, None)
        else:
            logger.debug("single confirm - method %r, delivery_tag %d",
                         method_name, delivery_tag)
            d = self._published_messages.pop(delivery_tag)
            deferred_method(d, None)

    def connectionLost(self, reason):

        logger.debug("connection lost due to %r", reason)
        self._ready_for_publish = None

        cstates = list(self._consumer_state.items())
        self._consumer_state.clear()

        for ct, cstate in cstates:
            queue_obj = cstate['queue_obj']
            no_ack = cstate['no_ack']
            if no_ack and not queue_obj.closed:
                logger.debug("put None to pika queue %r", queue_obj)
                queue_obj.put(None)
            elif not queue_obj.closed and not no_ack:
                logger.debug("close pika queue %r", queue_obj)
                queue_obj.close(reason)

        self._fail_published_messages(reason)
        TwistedProtocolConnection.connectionLost(self, reason)

    def _fail_published_messages(self, reason):
        m2f = list(self._published_messages.itervalues())
        self._published_messages.clear()
        for d in m2f:
            d.errback(reason)

    def publishMessage(
            self, exchange, routing_key, body,
            message_ttl=None,
            content_type=None, properties=None, confirm=True):

        if not self._ready_for_publish:
            raise _NotReadyForPublish("not ready for publish - channel in wrong state")

        data = serialize(body, content_type)
        logger.debug("publish message, msg %r, exhange %r, rk %r, props %r",
                     data, exchange, routing_key, properties)

        p = _BasicProperties(**(properties or {}))
        if content_type:
            p.content_type = content_type
        if message_ttl is not None:
            p.expiration = str(int(message_ttl))

        if confirm and self._safewrite_channel is not None:
            self._publish_delivery_tag_counter += 1
            delivery_tag = self._publish_delivery_tag_counter
            d = defer.Deferred()
            self._published_messages[delivery_tag] = d
            logger.debug("safe-publish, exc %r, rk %r: %r", exchange, routing_key, data)
            self._safewrite_channel.basic_publish(exchange, routing_key, data, properties=p)
            logger.debug("delivery tag is %r", delivery_tag)
            return d
        elif confirm:
            raise MethodNotImplemented("server doesn't support 'puslish confirm'")
        else:
            self._write_channel.basic_publish(exchange, routing_key, data, properties=p)
            return defer.succeed(None)

    @defer.inlineCallbacks  # noqa
    def _queueCounsumingLoop(self, consumer_tag, queue, callback, no_ack, parallel=0):

        if parallel >= 0:
            semaphore = defer.DeferredSemaphore(tokens=(parallel or 1))
        else:
            semaphore = None
        connection_done = False

        while 1:

            logger.debug("ct %s - waiting for msgs...", consumer_tag)
            if semaphore:
                logger.debug("acquire queue semaphore (ct %s)...", consumer_tag)
                yield semaphore.acquire()

            try:
                msg = yield queue.get()
            except _PikaQueueUnconsumed:
                logger.debug("stop consuming loop - queue unconsumed, ct %s", consumer_tag)
                break
            except ConnectionDone as e:
                connection_done = True
                logger.debug("pika queue closed due to %r", e)
                break
            except Exception:
                logger.exception("pika queue unexpectedly closed")
                break

            if not msg:
                logger.debug("found terminator %r in pika queue %s", msg, consumer_tag)
                break

            d = self._processIncomingMessage(msg, queue, callback, no_ack)

            if semaphore:
                def after(x):
                    semaphore.release()
                    return x
                d.addBoth(after)

        self._cleanupConsumingQueue(
            consumer_tag, queue,
            do_reject=(not connection_done and not no_ack),
        )

        if not queue.closed:
            logger.debug("close pika queue %r", queue)
            queue.close(_PikaQueueUnconsumed())

        logger.debug("queue consuming loop stopped, consumer_tag %r", consumer_tag)

    def _cleanupConsumingQueue(self, consumer_tag, queue, do_reject=True):

        logger.debug("clear consuming state for queue %r", queue)
        queue_pending = queue.pending[:]
        del queue.pending[:]

        if do_reject:
            for ch, deliver, _, _ in queue_pending:
                dt = deliver.delivery_tag
                logger.debug("nack message, delivery tag %r", dt)
                ch.basic_reject(delivery_tag=dt)

        rej_tasks = self._delayed_requeue_tasks.pop(consumer_tag, {})
        for dt, (t, ch) in rej_tasks.items():
            if do_reject:
                t.cancel()
                logger.debug("nack message, delivery tag %r", dt)
                ch.basic_reject(delivery_tag=dt)
            else:
                t.cancel()
                logger.debug("cancel nacking task, delivery tag %r", dt)

        logger.debug("consuming state for queue %r was cleared", queue)

    def _processIncomingMessage(self, msg, queue, callback, no_ack):

        ch, deliver, props, body = msg
        delivery_tag = deliver.delivery_tag
        amqp_msg = AMQPMessage(deliver=deliver, properties=props, body=body)
        logger.debug(
            "incoming message, body %r, queue %r, delivery_tag %r",
            body, queue, delivery_tag)

        d = defer.maybeDeferred(callback, amqp_msg)
        logger.debug(
            "incoming message, body %r, queue %r,"
            "delivery_tag %r - callback invoked...",
            body, queue, delivery_tag)

        def err(e):
            logger.error("fail to process msg %r - error %s", msg, e)
            if no_ack:
                return None
            if e.check(ConnectionDone):
                logger.debug("no active connection - we can't nack message")
            else:
                self._handleFailedIncomingMessage(ch, amqp_msg)

        def ack(x):
            if not no_ack:
                logger.debug("send ack, delivery tag %r", delivery_tag)
                ch.basic_ack(delivery_tag)

        d.addCallbacks(ack, err)
        return d

    def _handleFailedIncomingMessage(self, ch, msg):

        delivery_tag = msg.delivery_tag
        consumer_tag = msg.consumer_tag
        redelivered = msg.redelivered

        cstate = self._consumer_state[consumer_tag]
        if not cstate:
            logger.debug("no consumer state for ct %r - skip msg failure", consumer_tag)
            return

        rej_type = cstate.get('on_error') or self.on_error

        rej_tasks_count = len(self._delayed_requeue_tasks.get(consumer_tag, ()))
        too_many_rejs = rej_tasks_count > self.requeue_max_count

        need_requeue = (
            rej_type == 'requeue_forever'
            or (rej_type == 'requeue_once' and not redelivered)
            or (rej_type == 'requeue_hold' and not redelivered)
        )
        need_reject = rej_type == 'reject' or (rej_type == 'requeue_once' and redelivered)

        # TODO: log exception

        if rej_type == 'do_nothing':
            logger.debug("eat message failure: %r", msg)
            return

        elif redelivered and rej_type == 'requeue_hold':
            logger.debug("message %r already has `redelivered` bit, "
                         "hold it (don't nack nor ack)", delivery_tag)
            return

        elif need_reject and too_many_rejs:
            logger.error("reject message without delay: %r", msg)
            ch.basic_reject(delivery_tag, requeue=False)

        elif need_requeue and too_many_rejs:
            logger.error("requeue message without delay: %r", msg)
            ch.basic_reject(delivery_tag, requeue=True)

        elif too_many_rejs and rej_type == 'default':
            logger.error("requeue message without delay: %r", msg)
            ch.basic_reject(delivery_tag, requeue=True)

        elif need_reject or need_requeue:
            msg_requeue_delay = cstate.get('requeue_delay') or self.requeue_delay

            logger.debug("schedule requeue for dt %r", delivery_tag)
            cstate = self._consumer_state.get(consumer_tag)

            if msg_requeue_delay:
                def reject_message():
                    logger.debug("reject/requeue message, dt %r", delivery_tag)
                    ch.basic_reject(delivery_tag, requeue=need_requeue)
                    m = self._delayed_requeue_tasks.get(consumer_tag)
                    if m is not None:
                        m.pop(delivery_tag, None)
                    if m is not None and not m:
                        del self._delayed_requeue_tasks[consumer_tag]

                fmrt = self._delayed_requeue_tasks.setdefault(consumer_tag, {})
                assert delivery_tag not in fmrt
                t = self.clock.callLater(msg_requeue_delay, reject_message)
                fmrt[delivery_tag] = t, ch
                logger.debug("fmrt task is %r, dt %r", t, delivery_tag)

            else:
                logger.debug("reject message, dt %r", delivery_tag)
                ch.basic_reject(delivery_tag, requeue=True)

        else:
            logger.error("unknown reject type %r", rej_type)

    def _generateConsumerTag(self):
        type(self).__consumer_tag_cnt += 1
        return "ct-%s" % self.__consumer_tag_cnt

    @defer.inlineCallbacks
    def consumeQueue(
            self, queue='', callback=None, no_ack=False,
            requeue_delay=None, on_error=None,
            consumer_tag=None, parallel=0,
            **kwargs):

        assert callback
        logger.info("consume queue '%s/%s'", self.virtual_host, queue)
        consumer_tag = consumer_tag or self._generateConsumerTag()

        ch = yield self.channel()
        if self.prefetch_count is not None:
            logger.debug("set qos prefetch_count to %d", self.prefetch_count)
            yield ch.basic_qos(prefetch_count=self.prefetch_count, all_channels=0)

        queue_obj, ct = yield ch.basic_consume(
            queue=queue, no_ack=no_ack, consumer_tag=consumer_tag,
            **kwargs)
        assert ct == consumer_tag

        ch.add_on_close_callback(functools.partial(self._on_consuming_channel_closed, ct))
        logger.debug("open channel (read) %r for ct %r", ch, ct)

        self._consumer_state[ct] = dict(
            channel=ch,
            queue_obj=queue_obj,
            ct=ct,
            no_ack=no_ack,
            callback=callback,
            parallel=parallel,
            kwargs=kwargs,
            queue=queue,
            requeue_delay=requeue_delay,
            on_error=on_error,
        )

        # pika don't wait 'ConsumeOk' message
        # HACK-1: run consuming-loop a bit later to avoid races
        self.clock.callLater(
            0.05, self._queueCounsumingLoop,
            ct, queue_obj, callback, no_ack=no_ack, parallel=parallel,
        )

        # HACK-2: simulate waiting of 'ConsumeOk'
        yield timed.sleep(0.1)

        logger.debug("consumer tag is %r", consumer_tag)
        defer.returnValue(ct)

    @defer.inlineCallbacks
    def consumeExchange(
            self,
            callback=None, exchange='', no_ack=False,
            parallel=0, consumer_tag=None, routing_key='',
            bind_arguments=None, queue_arguments=None,
            requeue_delay=None,
            on_error=None,
    ):
        consumer_tag = consumer_tag or self._generateConsumerTag()

        logger.debug("declare exclusive queue")
        queue = "twoost-ec-%s" % uuid.uuid4().hex
        queue_method = yield self.declareQueue(
            queue=queue,
            auto_delete=True,
            arguments=queue_arguments,
            exclusive=True,
        )
        queue = queue_method.method.queue

        logger.debug("bind exclusive queue %r to exchange %r", queue, exchange)
        yield self.bindQueue(
            exchange=exchange,
            queue=queue,
            arguments=bind_arguments,
            routing_key=routing_key,
        )

        logger.debug("consume exchange %r, queue %r", exchange, queue)
        ct = yield self.consumeQueue(
            callback=callback,
            queue=queue,
            consumer_tag=consumer_tag,
            parallel=parallel,
            no_ack=no_ack,
            requeue_delay=requeue_delay,
            on_error=on_error,
        )

        defer.returnValue(ct)

    @defer.inlineCallbacks
    def cancelConsuming(self, consumer_tag):

        logger.debug("cancel consuming, consumer_tag %s", consumer_tag)

        cstate = self._consumer_state.pop(consumer_tag, {})
        ch = cstate.get('channel')

        if ch:
            logger.debug("send basic.cancel method, ct %r", consumer_tag)
            c = yield ch.basic_cancel(consumer_tag=consumer_tag)
        else:
            logger.error("can't cancel reading consuming for %s", consumer_tag)
            c = None

        queue_obj = cstate.get('queue_obj')
        if queue_obj and not queue_obj.closed:
            no_ack = cstate.get('no_ack')
            if no_ack:
                logger.debug("put None to pika queue %r", queue_obj)
                queue_obj.put(None)
            else:
                logger.debug("close pika queue %r", queue_obj)
                queue_obj.close(_PikaQueueUnconsumed())
        else:
            logger.debug("no queue_obj for ct %r", consumer_tag)

        defer.returnValue(c)

    # declare schema

    def declareQueue(
            self, queue, passive=False, durable=False,
            message_ttl=None, dead_letter_exchange=None, dead_letter_exchange_rk=None,
            exclusive=False, auto_delete=False, arguments=None):

        logger.info(
            "declare queue '%s/%s' (passive=%d, "
            "durable=%d, exclusive=%d, auto_delete=%d)",
            self.virtual_host,
            queue, passive, durable, exclusive, auto_delete)

        if message_ttl is not None:
            arguments = dict(arguments or ())
            arguments['x-message-ttl'] = int(message_ttl)

        if dead_letter_exchange is not None:
            arguments = dict(arguments or ())
            arguments['x-dead-letter-exchange'] = dead_letter_exchange
            if dead_letter_exchange_rk:
                arguments['x-dead-letter-routing-key'] = dead_letter_exchange_rk

        return self._write_channel.queue_declare(
            queue=queue, passive=passive, durable=durable, exclusive=exclusive,
            auto_delete=auto_delete, arguments=arguments,
        )

    def declareExchange(
            self, exchange, exchange_type='direct', passive=False,
            durable=False, auto_delete=False, internal=False, arguments=None):

        logger.info(
            "declare exchange '%s/%s': type=%r, passive=%d, "
            "durable=%d, auto_delete=%d, internal=%d",
            self.virtual_host,
            exchange, exchange_type, passive, durable, auto_delete, internal)

        return self._write_channel.exchange_declare(
            exchange=exchange, passive=passive,
            durable=durable, exchange_type=exchange_type,
            auto_delete=auto_delete, arguments=arguments, internal=internal)

    def bindQueue(self, exchange, queue, routing_key='', arguments=None):
        logger.info(
            "bind exchange '%s/%s' to queue %r, routing key %r",
            self.virtual_host, exchange, queue, routing_key)

        return self._write_channel.queue_bind(
            queue=queue, exchange=exchange,
            routing_key=routing_key, arguments=arguments)

    def bindExchange(self, source, destination, routing_key='', arguments=None):
        logger.info(
            "bind exchange '%s/%s' to exchange %r, routing key %r",
            destination, source, routing_key)
        return self._write_channel.exchange_bind(
            self.virtual_host,
            destination=destination, source=source,
            routing_key=routing_key, arguments=arguments)

    def logPrefix(self):
        return 'amqp'


class AMQPFactory(pclient.PersistentClientFactory):

    protocol = _AMQPProtocol
    _protocol_instance = None

    def __init__(
            self,
            schema=None,
            vhost=None,
            user=None,
            password=None,
            heartbeat=None,
            prefetch_count=None,
            requeue_delay=120,
            on_error=None,
            **kwargs
    ):
        # defaults for _AMQPProtocol

        self.prefetch_count = prefetch_count
        self.on_error = on_error
        self.requeue_delay = requeue_delay

        self._protocol_parameters = {
            'virtual_host': vhost,
            'credentials': _PlainCredentials(user or 'guest', password or 'guest'),
            'heartbeat_interval': None,
            'ssl': None,
            'heartbeat_interval': heartbeat,
            'connection_attempts': None,
        }

        self.schema = schema

    def logPrefix(self):
        return 'amqp'

    def notifyHandshaking(self):
        # deprecated
        if self._protocol_instance:
            return self.notifyProtocolReady()
        else:
            return defer.fail(Exception("No client"))

    def buildProtocol(self, addr):
        logger.debug("build amqp protocol, params %r", self._protocol_parameters)
        p = self.protocol(
            parameters=self._protocol_parameters,
            schema=self.schema,
            prefetch_count=self.prefetch_count,
            requeue_delay=self.requeue_delay,
            on_error=self.on_error,
        )
        p.factory = self
        self._protocol_instance = p
        return p

    def clientConnectionLost(self, connector, reason):
        if self._protocol_instance and self._protocol_instance.heartbeat:
            logger.debug("stop heartbeating")
            self._protocol_instance.heartbeat.stop()
        self._protocol_instance = None
        pclient.PersistentClientFactory.clientConnectionLost(self, connector, reason)


# -- config parsing

@zope.interface.implementer(IAMQPSchema)
class schemaFromDict(object):

    """Loads AMQP schema from python dict."""

    def __init__(self, config):
        ks = set(config.keys()) - set(['exchange', 'queue', 'bind'])
        if ks:
            raise ValueError("Invalid schema dict: unexpected keys %r", ks)
        self.config = config

    @defer.inlineCallbacks
    def declareSchema(self, builder):
        logger.debug("Load schema from config %r", self.config)

        for exchange, props in self.config.get('exchange', {}).items():
            # small quirk
            props = props or {}
            if 'type' in props and 'exchange_type' not in props:
                props['exchange_type'] = props.pop('type')

            yield builder.declareExchange(exchange=exchange, **props)

        for queue, props in self.config.get('queue', {}).items():
            props = props or {}
            yield builder.declareQueue(queue=queue, **props)

        for bind in self.config.get('bind', ()):
            if isinstance(bind, (tuple, list)):
                yield builder.bindQueue(*bind)
            else:
                yield builder.bindQueue(**bind)

        for bind in self.config.get('bind_exchange', ()):
            if isinstance(bind, (tuple, list)):
                yield builder.bindExchange(*bind)
            else:
                yield builder.bindExchange(**bind)


components.registerAdapter(schemaFromDict, dict, IAMQPSchema)


def loadSchema(schema):
    if isinstance(schema, basestring):
        schema = reflect.namedAny(schema)
    if not hasattr(schema, 'declareSchema'):
        schema = IAMQPSchema(schema)
    return schema


# --- integration with app-framework

class _BaseConsumer(service.Service):

    cancel_consuming_timeout = 10
    cancel_message_timeout = 20
    _protocol_instance = None
    consumer_tag = None

    def __init__(
            self,
            callback,
            parallel=0,
            no_ack=False,
            deserialize=True,
            requeue_delay=None,
            on_error=None,
    ):

        self.callback = callback
        self.deserialize = deserialize
        self.parallel = parallel
        self.no_ack = no_ack
        self.requeue_delay = requeue_delay
        self.on_error = on_error
        self._active_callbacks = {}
        self._active_callbacks_cnt = 0
        self._consume_deferred = None

    @defer.inlineCallbacks
    def _cancelActiveCallbacks(self):

        cancelled = [0]
        ds = list(self._active_callbacks.values())
        self._active_callbacks.clear()

        def ignore_cancelled_error(f):
            f.trap(defer.CancelledError)
            cancelled[0] += 1

        def log_error(f):
            logger.error("callback %s failed: %s", self.callback, f)

        for d in ds:
            timed.timeoutDeferred(d, self.cancel_message_timeout)
            d.addErrback(ignore_cancelled_error)
            d.addErrback(log_error)
            d.cancel()

        yield defer.gatherResults(ds)
        if cancelled[0]:
            logger.debug("cancelled %d msgs for callback %s", self.callback)

    def startService(self):
        if self.running:
            logger.debug("service %r already started", self)
            return
        logger.debug("start service %s", self)
        service.Service.startService(self)
        p = self.parent.amqp_service.getProtocol()
        if p:
            self.clientProtocolReady(p)

    @defer.inlineCallbacks
    def stopService(self):

        if not self.running:
            return
            logger.debug("service %r already stopped", self)

        if self._consume_deferred:
            logger.debug("wait previous consuming request...")
            timed.timeoutDeferred(self._consume_deferred, self.cancel_consuming_timeout)
            try:
                yield self._consume_deferred
            except Exception:
                logger.exception("upps")

            logger.debug("too quick consume-unconsume - sleep for 0.5 sec")
            yield timed.sleep(0.5)

        logger.debug("stop service %s", self)
        p1 = self._protocol_instance
        p2 = self.parent.amqp_service.getProtocol()

        if p1 is p2 and self.consumer_tag:
            logger.debug("protocol didn't change - cancel consuming")
            d = p1.cancelConsuming(self.consumer_tag)
            timed.timeoutDeferred(d, self.cancel_consuming_timeout)
            try:
                yield d
            except Exception:
                logger.exception("Can't cancel consuming")

        yield self._cancelActiveCallbacks()
        yield defer.maybeDeferred(service.Service.stopService, self)

    def clientProtocolReady(self, protocol):

        if not self.running:
            logger.debug("consumer %r stopped, skip new protocol", self)
            return

        # setup _protocol_instance *before* consuming
        self._protocol_instance = protocol
        self._consume_deferred = self._consume(protocol)
        self._consume_deferred.addCallback(
            self.consumingStarted
        ).addErrback(
            self.consumingFailed
        )

    def consumingStarted(self, consumer_tag):
        logger.debug("consuming started, ct %r", consumer_tag)
        self.consumer_tag = consumer_tag
        self._consume_deferred = None

    def consumingFailed(self, fail):
        logger.error("failed to consume %s: %s", self, fail)
        self.consumer_tag = None
        self._consume_deferred = None

    def onMessage(self, msg):
        logger.debug("receive message %r", msg)

        self._active_callbacks_cnt += 1
        cid = self._active_callbacks_cnt

        def remove_ac(x):
            self._active_callbacks.pop(cid, None)
            return x

        data = deserialize(msg.body, msg.content_type) if self.deserialize else msg
        d = self._active_callbacks[cid] = defer.maybeDeferred(self.callback, data)
        return d.addBoth(remove_ac)


class _QueueConsumer(_BaseConsumer):

    """Consumes AMQP queue & runs callback."""

    def __init__(self, queue, callback, *args, **kwargs):
        _BaseConsumer.__init__(self, callback, *args, **kwargs)
        self.queue = queue

    def _consume(self, protocol):
        return protocol.consumeQueue(
            queue=self.queue,
            callback=self.onMessage,
            parallel=self.parallel,
            no_ack=self.no_ack,
            requeue_delay=self.requeue_delay,
            on_error=self.on_error,
        )


class _ExchangeConsumer(_BaseConsumer):

    """Consumes AMQP exchange & runs callback."""
    def __init__(self, exchange, callback, routing_key='', *args, **kwargs):
        _BaseConsumer.__init__(self, callback, *args, **kwargs)
        self.exchange = exchange
        self.routing_key = routing_key

    def _consume(self, protocol):
        return protocol.consumeExchange(
            exchange=self.exchange,
            callback=self.onMessage,
            parallel=self.parallel,
            no_ack=self.no_ack,
            requeue_delay=self.requeue_delay,
            on_error=self.on_error,
            routing_key=self.routing_key,
        )


class _ConsumersContainer(service.MultiService):

    def __init__(self, amqp_service):
        service.MultiService.__init__(self)
        self.amqp_service = amqp_service


class AMQPService(object, pclient.PersistentClientService):
    # amqp service contains all conusumers as subservices

    name = 'amqp'
    protocolProxiedMethods = ['publishMessage']

    def __init__(self, *args, **kwargs):
        pclient.PersistentClientService.__init__(self, *args, **kwargs)
        self.consumer_services = _ConsumersContainer(self)

    def startService(self):
        pclient.PersistentClientService.startService(self)
        self.consumer_services.startService()

    def privilegedStartService(self):
        pclient.PersistentClientService.privilegedStartService(self)
        self.consumer_services.privilegedStartService()

    @defer.inlineCallbacks
    def stopService(self):
        yield self.consumer_services.stopService()
        yield defer.maybeDeferred(pclient.PersistentClientService.stopService, self)

    def needToRetryProtocolCall(self, f):
        return f.check(ConnectionDone) or f.check(_NotReadyForPublish)

    def clientConnectionLost(self, reason):
        p = self.getProtocol()
        if p and p.heartbeat:
            logger.debug("stop heartbeating")
            p.heartbeat.stop()
        return pclient.PersistentClientService.clientConnectionLost(self, reason)

    def clientProtocolReady(self, protocol):
        pclient.PersistentClientService.clientProtocolReady(self, protocol)
        for ss in self.consumer_services.services:
            ss.clientProtocolReady(protocol)

    def setupQueueConsuming(self, queue, callback, no_ack=False, parallel=0,
                            deserialize=True, requeue_delay=None, on_error=None):

        logger.debug("setup queue consuming for conn %r, queue %r", self, queue)
        qc = _QueueConsumer(
            callback=callback,
            queue=queue,
            parallel=parallel,
            no_ack=no_ack,
            deserialize=deserialize,
            requeue_delay=requeue_delay,
            on_error=on_error,
        )
        qc.setServiceParent(self.consumer_services)
        return qc

    def setupExchangeConsuming(self, exchange, callback, routing_key='', requeue_delay=None,
                               parallel=0, deserialize=True, no_ack=False, on_error=None):

        logger.debug("setup exchange consuming for conn %r, exch %r", self, exchange)
        qc = _ExchangeConsumer(
            callback=callback,
            exchange=exchange,
            routing_key=routing_key,
            deserialize=deserialize,
            no_ack=no_ack,
            parallel=parallel,
            requeue_delay=requeue_delay,
            on_error=on_error,
        )
        qc.setServiceParent(self.consumer_services)
        return qc

    def unsetupConsuming(self, consumer):
        assert isinstance(consumer, _BaseConsumer)
        self.consumer_services.removeService(consumer)

    def makeSender(self, exchange, routing_key=None, routing_key_fn=None,
                   content_type='json', confirm=True):

        assert routing_key is None or routing_key_fn is None
        logger.debug(
            "build sender callback for conn %r, " "exchange %r, ctype %s, confirm flag %r",
            self, exchange, content_type, confirm)

        def send(data):
            rk = routing_key or (routing_key_fn and routing_key_fn(data)) or ''
            return self.publishMessage(
                exchange=exchange,
                routing_key=rk,
                body=data,
                content_type=content_type,
                confirm=confirm,
            )
        return send


class AMQPCollectionService(pclient.PersistentClientsCollectionService):

    name = 'amqps'
    clientService = AMQPService
    factory = AMQPFactory

    defaultParams = {
        'port': 5672,
        'host': "localhost",
    }

    def setupQueueConsuming(self, connection, *args, **kwargs):
        return self[connection].setupQueueConsuming(*args, **kwargs)

    def setupExchangeConsuming(self, connection, *args, **kwargs):
        return self[connection].setupExchangeConsuming(*args, **kwargs)

    def makeSender(self, connection, *args, **kwargs):
        return self[connection].makeSender(*args, **kwargs)
