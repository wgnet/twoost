# coding: utf-8

from __future__ import print_function, division

"""
Simple Twisted-style abstraction around AMQP pika library.
"""

import pickle
import json
import random
import functools

import zope.interface

from twisted.internet import defer
from twisted.internet.error import ConnectionDone
from twisted.python import failure, components, reflect
from twisted.application import service

from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.spec import BasicProperties as _BasicProperties
from pika.connection import ConnectionParameters as _ConnectionParameters
from pika.credentials import PlainCredentials as _PlainCredentials
from pika.exceptions import MethodNotImplemented, ChannelClosed

from twoost import timed
from twoost._misc import get_attached_clock
from twoost.pclient import PersistentClientFactory, PersistentClientService

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
            queue, passive=False, durable=False,
            exclusive=False, auto_delete=False, arguments=None):
        pass

    def declareExchange(
            exchange, exchange_type='direct', passive=False,
            durable=False, auto_delete=False, internal=False, arguments=None):
        pass

    def bindQueue(queue, exchange, routing_key='', arguments=None):
        pass

    def bindExchange(destination, source, routing_key='', arguments=None):
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


def deserialize(data, content_type):
    logger.debug("deserialize data %r, content_type %r", data, content_type)
    if not content_type:
        return data
    if content_type in ['json', 'application/json']:
        return json.loads(data)
    elif content_type in ['pickle', 'application/pickle']:
        return pickle.loads(data)
    else:
        return data


def serialize(data, content_type):
    logger.debug("serialize data %r, content_type %r", data, content_type)
    if not content_type:
        return data
    if content_type in ['json', 'application/json']:
        return json.dumps(data)
    elif content_type in ['pickle', 'application/pickle']:
        return pickle.dumps(data)
    else:
        return data


# ---

class _PikaQueueUnconsumed(Exception):
    pass


class _NotReadyForPublish(Exception):
    pass


class _SchemaBuilderProxy(components.proxyForInterface(IAMQPSchemaBuilder)):
    pass


class _AMQPProtocol(TwistedProtocolConnection, object):

    DELAYED_REJECTIONS_LIMIT = 5000

    def __init__(
            self, parameters,
            schema=None,
            prefetch_count=None,
            message_rejection_delay=120,
            preserve_double_redelivery=True,
    ):

        logger.debug("construct new _AMQPProtocol (id = %r)...", id(self))
        TwistedProtocolConnection.__init__(
            self, _ConnectionParameters(**parameters))

        self.schema = schema
        self.prefetch_count = prefetch_count
        self.message_rejection_delay = message_rejection_delay
        self.preserve_double_redelivery = preserve_double_redelivery

        self._on_handshaking_made = defer.Deferred()

        self._published_messages = {}
        self._exclusive_queues = {}
        self._consume_state = {}

        # FIXME: replace with `ttl`
        self._failed_msg_rej_tasks = {}

        self.ready_for_publish = False

    def connectionMade(self):
        logger.debug("amqp connection was made")
        TwistedProtocolConnection.connectionMade(self)
        self.factory.resetDelay()
        self.ready.addCallback(lambda _: self.handshakingMade())
        self.ready.addErrback(self.handshakingFailed)

    def handshakingFailed(self, failure):
        logger.error("handshaking failed - disconnect due to %s", failure)
        self.transport.loseConnection()
        self._on_handshaking_made.errback(failure)

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

        logger.info("handshaking with server was made")

        yield self._open_write_channel()
        yield self._open_safewrite_channel()

        if self.schema:
            logger.debug("declare schema...")
            yield defer.maybeDeferred(self.schema.declareSchema, _SchemaBuilderProxy(self))
            logger.info("amqp schema has been declared")

        self.ready_for_publish = True
        self._on_handshaking_made.callback(True)

    def _on_consuming_channel_closed(self, ct, channel, reply_code, reply_text):
        logger.error(
            "server closed channel, ct %r, reply_code %s, reply_text %s!",
            ct, reply_code, reply_text)
        s = self._consume_state.pop(ct, None)
        if not s:
            return
        logger.debug("reconsume queue, prev state is %r", s)
        self.consumeQueue(
            queue=s['queue'],
            callback=s['callback'],
            no_ack=s['callback'],
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
        deferred_method = (
            defer.Deferred.callback if ack
            else defer.Deferred.errback)

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
        self.ready_for_publish = None

        for ct, cstate in self._consume_state.items():
            queue_obj = cstate['queue_obj']
            no_ack = cstate['no_ack']
            if no_ack and not queue_obj.closed:
                logger.debug("put None to pika queue %r", queue_obj)
                queue_obj.put(None)
            elif not queue_obj.closed and not no_ack:
                logger.debug("close pika queue %r", queue_obj)
                queue_obj.close(reason)

        self._consume_state.clear()
        self._fail_published_messages(reason)
        TwistedProtocolConnection.connectionLost(self, reason)

    def _fail_published_messages(self, reason):
        m2f = list(self._published_messages.itervalues())
        self._published_messages.clear()
        for d in m2f:
            d.errback(reason)

    def publishMessage(
            self, exchange, routing_key, body,
            content_type=None, properties=None, confirm=True):

        if not self.ready_for_publish:
            raise _NotReadyForPublish("not ready for publish - channel in wrong state")

        data = serialize(body, content_type)
        logger.debug("publish message, msg %r, exhange %r, rk %r, props %r",
                     data, exchange, routing_key, properties)

        p = _BasicProperties(**(properties or {}))
        if content_type:
            p.content_type = content_type

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

        rej_tasks = self._failed_msg_rej_tasks.pop(consumer_tag, {})
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

        if no_ack:
            return defer.succeed(None)

        def err(e):
            logger.error("fail to process msg %r - error %s", msg, e)
            if e.check(ConnectionDone):
                logger.debug("no active connection - we can't nack message")
            else:
                self._handleFailedIncomingMessage(ch, amqp_msg)

        def ack(x):
            logger.debug("send ack, delivery tag %r", delivery_tag)
            ch.basic_ack(delivery_tag)

        d.addCallbacks(ack, err)
        return d

    def _handleFailedIncomingMessage(self, ch, msg):

        delivery_tag = msg.delivery_tag
        consumer_tag = msg.consumer_tag
        redelivered = msg.redelivered

        rej_tasks_count = len(
            self._failed_msg_rej_tasks.get(consumer_tag, ()))

        if redelivered or rej_tasks_count > self.DELAYED_REJECTIONS_LIMIT:
            if self.preserve_double_redelivery:
                logger.debug(
                    "message %r already has `redelivered` bit, "
                    "hold it (don't nack nor ack)",
                    delivery_tag)
            else:
                logger.error("reject message without redelivery flag: %r", msg)
                self.basic_reject(delivery_tag, requeue=False)
        else:
            def nack_failed_message():
                logger.debug("reject message, dt %r", delivery_tag)
                ch.basic_reject(delivery_tag, requeue=True)
                m = self._failed_msg_rej_tasks.get(consumer_tag)
                if m is not None:
                    m.pop(delivery_tag, None)
                if m is not None and not m:
                    del self._failed_msg_rej_tasks[consumer_tag]

            logger.debug("schedule rejection for dt %r", delivery_tag)
            delay = random.normalvariate(
                self.message_rejection_delay,
                self.message_rejection_delay * 0.05)

            clock = get_attached_clock(self)
            t = clock.callLater(max(delay, 0), nack_failed_message)

            fmrt = self._failed_msg_rej_tasks.setdefault(consumer_tag, {})
            assert delivery_tag not in fmrt
            fmrt[delivery_tag] = t, ch

            logger.debug("fmrt task is %r, dt %r", t, delivery_tag)

    @defer.inlineCallbacks
    def consumeQueue(
            self, queue='', callback=None, no_ack=False,
            consumer_tag=None, parallel=0, **kwargs):

        assert callback
        logger.info("consume messages from queue %r" % queue)

        ch = yield self.channel()
        if self.prefetch_count is not None:
            logger.debug("set qos prefetch_count to %d", self.prefetch_count)
            yield ch.basic_qos(prefetch_count=self.prefetch_count, all_channels=0)

        queue_obj, ct = yield ch.basic_consume(
            queue=queue, no_ack=no_ack, consumer_tag=consumer_tag,
            **kwargs)

        ch.add_on_close_callback(functools.partial(self._on_consuming_channel_closed, ct))
        logger.debug("open channel (read) %r for ct %r", ch, ct)

        self._consume_state[ct] = dict(
            channel=ch,
            queue_obj=queue_obj,
            ct=ct,
            no_ack=no_ack,
            callback=callback,
            parallel=parallel,
            kwargs=kwargs,
            queue=queue,
        )

        self._queueCounsumingLoop(
            ct, queue_obj, callback, no_ack=no_ack, parallel=parallel)

        logger.debug("consumer tag is %r", consumer_tag)

        # FIXME: pika don't wait for ConsumeOK, simulate it
        yield timed.sleep(0.01)

        defer.returnValue(ct)

    @defer.inlineCallbacks
    def consumeExchange(
            self,
            callback=None, exchange='', no_ack=False,
            parallel=0, consumer_tag=None, routing_key='',
            bind_arguments=None, queue_arguments=None,
    ):

        logger.debug("declare exclusive queue")
        queue_method = yield self.declareQueue(
            queue='', exclusive=True, arguments=queue_arguments)
        queue = queue_method.method.queue

        logger.debug("bind exclusive queue %r to exchange %r", queue, exchange)
        yield self.bindQueue(
            queue=queue, exchange=exchange,
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
        )

        self._exclusive_queues[ct] = queue, no_ack

    @defer.inlineCallbacks
    def cancelConsuming(self, consumer_tag):

        logger.debug("cancel consuming, consumer_tag %s", consumer_tag)

        cstate = self._consume_state.pop(consumer_tag, {})
        ch = cstate.get('channel')
        if ch:
            logger.debug("send basic.cancel method, ct %r", consumer_tag)
            c = yield ch.basic_cancel(consumer_tag=consumer_tag)
        else:
            logger.error("can't cancel reading consuming for %s", consumer_tag)
            c = None

        queue, no_ack = self._exclusive_queues.pop(consumer_tag, (None, None))
        if queue:
            logger.debug("drop exclusive queue %r", queue)
            try:
                if no_ack:
                    yield self._write_channel.queue_delete(queue=queue)
                else:
                    yield self._safewrite_channel.queue_delete(queue=queue)
            except:
                logger.exception("can't delete queue %r", queue)

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
            exclusive=False, auto_delete=False, arguments=None):

        logger.info(
            "declare queue %r (passive=%d, "
            "durable=%d, exclusive=%d, auto_delete=%d)",
            queue, passive, durable, exclusive, auto_delete)

        return self._write_channel.queue_declare(
            queue=queue, passive=passive, durable=durable, exclusive=exclusive,
            auto_delete=auto_delete, arguments=arguments,
        )

    def declareExchange(
            self, exchange, exchange_type='direct', passive=False,
            durable=False, auto_delete=False, internal=False, arguments=None):

        logger.info(
            "declare exchange %r (type=%r, passive=%d, "
            "durable=%d, auto_delete=%d, internal=%d)",
            exchange, exchange_type, passive, durable, auto_delete, internal)

        return self._write_channel.exchange_declare(
            exchange=exchange, passive=passive,
            durable=durable, exchange_type=exchange_type,
            auto_delete=auto_delete, arguments=arguments, internal=internal)

    def bindQueue(self, queue, exchange, routing_key='', arguments=None):
        logger.info(
            "bind exchange %r to queue %r (routing key is %r)",
            exchange, queue, routing_key)
        return self._write_channel.queue_bind(
            queue=queue, exchange=exchange,
            routing_key=routing_key, arguments=arguments)

    def bindExchange(self, destination, source, routing_key='', arguments=None):
        logger.info(
            "bind exchange %r to exchange %r (routing key is %r)",
            destination, source, routing_key)
        return self._write_channel.exchange_bind(
            destination=destination, source=source,
            routing_key=routing_key, arguments=arguments)

    def logPrefix(self):
        return 'amqp'


# -- client factory

class AMQPClient(PersistentClientFactory):

    protocol = _AMQPProtocol
    proxiedMethods = ['publishMessage']
    retryOnErrors = [ConnectionDone, _NotReadyForPublish]

    _protocol_parameters = None
    __consumer_tag_counter = 0

    def __init__(
            self,
            schema=None,
            vhost=None,
            user=None,
            password=None,
            heartbeat=None,
            prefetch_count=None,
            message_rejection_delay=120,
            preserve_double_redelivery=True,
            disconnect_period=10800,
            max_outgoing_queue_size=50000,
            max_outgoing_messages_delay=250,
    ):

        self.schema = schema
        self.disconnectDelay = disconnect_period
        self.prefetch_count = prefetch_count
        self.message_rejection_delay = message_rejection_delay
        self.preserve_double_redelivery = preserve_double_redelivery
        self.max_outgoing_queue_size = max_outgoing_queue_size
        self.max_outgoing_messages_delay = max_outgoing_messages_delay

        self._protocol_parameters = {
            'virtual_host': vhost,
            'credentials': (
                _PlainCredentials(user or 'guest', password or 'guest')),
            'heartbeat_interval': None,
            'ssl': None,
            'heartbeat_interval': heartbeat,
            'connection_attempts': None,
        }

        self.consuming_callbacks = {}

        # transient state
        self._consumed_queues_is_ready = {}
        self._hm_deffers = []
        self._handshaking_made = False
        self._client_prepared = False
        self.client = None

    def logPrefix(self):
        return 'amqp'

    def _clientHandshakingMade(self, client):
        self.clientReady(client)

        self._handshaking_made = True
        self._prepareClient()
        logger.debug("client handshaking was made")

        for c in self._hm_deffers:
            if not c.called:
                c.callback(True)

    def notifyHandshaking(self):
        d = defer.Deferred()
        if self._handshaking_made:
            d.callback(True)
        else:
            self._hm_deffers.append(d)
        return d

    def buildProtocol(self, addr):

        logger.debug("build amqp protocol, params %r", self._protocol_parameters)
        p = self.protocol(
            parameters=self._protocol_parameters,
            schema=self.schema, prefetch_count=self.prefetch_count,
            message_rejection_delay=self.message_rejection_delay,
            preserve_double_redelivery=self.preserve_double_redelivery,
        )

        self._handshaking_made = False
        self._client_prepared = False

        self.client = p
        self.client.factory = self

        p._on_handshaking_made.addCallback(
            lambda _: self._clientHandshakingMade(p))

        def fail_handshaking(e):
            logger.error("handshaking failed due to %s", e)

        p._on_handshaking_made.addErrback(fail_handshaking)
        return p

    def _setHandshakingFlag(self, x):
        self._handshaking_made = True
        return x

    def clientConnectionLost(self, connector, reason):
        if self.client and self.client.heartbeat:
            logger.debug("stop heartbeating")
            self.client.heartbeat.stop()
        PersistentClientFactory.clientConnectionLost(self, connector, reason)

    @defer.inlineCallbacks
    def _prepareClient(self):
        logger.debug("setup consuming...")

        for consumer_tag, consume in self.consuming_callbacks.items():
            logger.debug(
                "invoke consuming callback for consumer tag %r",
                consumer_tag)
            yield consume(self.client)
            d = self._consumed_queues_is_ready.get(consumer_tag)
            if d and not d.called:
                d.callback(True)

        # all saved queues are consumed
        # allow `self._consumeAndSaveCallback` to send AMQP packages
        self._client_prepared = True

        for consumer_tag, d in self._consumed_queues_is_ready.items():
            if not d.called:
                logger.debug(
                    "found %r in `_consumed_queues_is_ready`"
                    " but not in `consuming_callbacks`", consumer_tag)
                d.errback(Exception("can't consume - bad state"))

    def _generateConsumerTag(self):
        logger.debug("generate new consumer tag")
        self.__consumer_tag_counter += 1
        consumer_tag = "ct-{0}".format(self.__consumer_tag_counter)
        self._consumed_queues_is_ready[consumer_tag] = defer.Deferred()
        return consumer_tag

    @defer.inlineCallbacks
    def _consumeAndSaveCallback(self, consumer_tag, consume):
        logger.debug("setup consuming, ct %s consumer_tag", consumer_tag)
        self.consuming_callbacks[consumer_tag] = consume
        if self.client and self._client_prepared:
            d = self._consumed_queues_is_ready[consumer_tag]
            try:
                yield consume(self.client)
                d.callback(True)
            except:
                d.errback(failure.Failure())
                raise
        defer.returnValue(consumer_tag)

    def consumeQueue(self, queue='', callback=None, **kwargs):

        consumer_tag = self._generateConsumerTag()
        logger.debug("consume from queue %r, contumer_tag %r, kwargs %r",
                     queue, consumer_tag, kwargs)

        def consume(amqp):
            logger.debug("consume queue %r with ct %s ...", queue, consumer_tag)
            return amqp.consumeQueue(
                queue=queue, callback=callback, consumer_tag=consumer_tag, **kwargs)

        return self._consumeAndSaveCallback(consumer_tag, consume)

    def consumeExchange(self, exchange='', callback=None, **kwargs):

        consumer_tag = self._generateConsumerTag()
        logger.debug("consume from exchange %r, contumer_tag %r, kwargs %r",
                     exchange, consumer_tag, kwargs)

        def consume(amqp):
            logger.debug("consume exchange %r with ct %s ...", exchange, consumer_tag)
            return amqp.consumeExchange(
                exchange=exchange, callback=callback, consumer_tag=consumer_tag, **kwargs)

        return self._consumeAndSaveCallback(consumer_tag, consume)

    @defer.inlineCallbacks
    def cancelConsuming(self, consumer_tag):
        logger.debug("client - cancel consuming, consumer_tag %r", consumer_tag)

        c = self.consuming_callbacks.pop(consumer_tag, None)
        if c is None:
            logger.warning("unknown consumer tag %r", consumer_tag)
            return

        if c and self.client:
            d = self._consumed_queues_is_ready.get(consumer_tag)
            if d:
                logger.debug("wait for queue with ct %s...", consumer_tag)
                consuming_installed = yield d
                if consuming_installed and self.client and self._handshaking_made:
                    # don't wait - no `yield`
                    yield self.client.cancelConsuming(consumer_tag)
                del self._consumed_queues_is_ready[consumer_tag]


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
                exchange, queue = bind
                bind = {'exchange': exchange, 'queue': queue}
            if 'queue' in bind and 'exchange' in bind:
                yield builder.bindQueue(**bind)
            elif 'destination' in bind and 'source' in bind:
                yield builder.bindExchange(**bind)
            else:
                raise Exception("invalid binding %r" % bind)


components.registerAdapter(schemaFromDict, dict, IAMQPSchema)


def loadSchema(schema):
    if isinstance(schema, basestring):
        schema = reflect.namedAny(schema)
    if not hasattr(schema, 'declareSchema'):
        schema = IAMQPSchema(schema)
    return schema


# --- integration with app-framework

class _BaseConsumer(service.Service):

    def __init__(self, client, callback, parallel=0,
                 no_ack=False, deserialize=True, **kwargs):
        self.client = client
        self.callback = callback
        self.deserialize = deserialize
        self.parallel = parallel
        self.no_ack = no_ack

    @defer.inlineCallbacks
    def startService(self):
        logger.debug("start service %s", self)
        service.Service.startService(self)
        self.consumer_key = yield self._consume()

    @defer.inlineCallbacks
    def stopService(self):
        logger.debug("stop service %s", self)
        yield self.client.cancelConsuming(self.consumer_key)
        service.Service.stopService(self)

    def onMessage(self, msg):
        logger.debug("receive message %r", msg)
        data = deserialize(msg.body, msg.content_type) if self.deserialize else msg
        return defer.maybeDeferred(self.callback, data)


class QueueConsumer(_BaseConsumer):

    """Consumes AMQP queue & runs callback."""

    def __init__(self, client, queue, callback, *args, **kwargs):
        _BaseConsumer.__init__(self, client, callback, *args, **kwargs)
        self.queue = queue

    def _consume(self):
        return self.client.consumeQueue(
            self.queue, self.onMessage,
            parallel=self.parallel, no_ack=self.no_ack,
        )


class ExchangeConsumer(_BaseConsumer):

    """Consumes AMQP exchange & runs callback."""
    def __init__(self, client, exchange, callback, *args, **kwargs):
        _BaseConsumer.__init__(self, client, callback, *args, **kwargs)
        self.exchange = exchange

    def _consume(self):
        return self.client.consumeExchange(
            self.exchange, self.onMessage, parallel=self.parallel, no_ack=self.no_ack,
        )


class AMQPService(PersistentClientService):

    name = 'amqps'
    factory = AMQPClient
    defaultPort = 5672

    # ---

    def setupQueueConsuming(
            self, connection, callback, queue,
            parallel=0, no_ack=False, deserialize=True,
    ):
        logger.debug("setup queue consuming for conn %r, queue %r", connection, queue)
        qc = QueueConsumer(
            client=self[connection],
            callback=callback, queue=queue,
            parallel=parallel, no_ack=no_ack,
            deserialize=deserialize,
        )
        self.addService(qc)

    def setupExchangeConsuming(
            self, connection, callback, exchange, no_ack=False,
            parallel=0, routing_key=None, bind_arguments=None, queue_arguments=None,
    ):
        logger.debug("setup exchange consuming for conn %r, exch %r", connection, exchange)

        qc = ExchangeConsumer(
            client=self[connection],
            callback=callback,
            exchange=exchange,
            no_ack=no_ack,
            parallel=parallel,
            routing_key=routing_key,
            bind_arguments=None,
            queue_arguments=None,
        )
        self.addService(qc)

    def makeSender(
            self, connection, exchange='', routing_key=None,
            routing_key_fn=None, content_type='json', confirm=True,
    ):
        assert routing_key is None or routing_key_fn is None

        logger.debug(
            "build sender callback for conn %r, " "exchange %r, ctype %s, confirm flag %r",
            connection, exchange, content_type, confirm)

        def send(data):
            rk = routing_key or (routing_key_fn and routing_key_fn(data)) or ''
            return self[connection].publishMessage(
                exchange=exchange,
                routing_key=rk,
                body=data,
                content_type=content_type,
                confirm=confirm,
            )
        return send
