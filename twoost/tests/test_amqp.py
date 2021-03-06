# coding: utf-8

from __future__ import print_function, division, absolute_import

import zope.interface

from twisted.internet import reactor, endpoints
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from twoost import amqp
from twoost.timed import sleep

import twisted.internet.base  # noqa

import logging
logger = logging.getLogger(__name__)


Q1 = 'test_mcstats_amqp_queue_1'
QX = 'test_mcstats_amqp_queue_x'

E1 = 'test_mcstats_amqp_exchange_1'
EX = 'test_mcstats_amqp_exchange_x'


class TestSchema(object):

    @defer.inlineCallbacks
    def declareSchema(self, b):

        yield b.declareQueue(queue=Q1, auto_delete=False)

        yield b.declareQueue(queue='mcs_amqp_q1')
        yield b.declareQueue(queue='mcs_amqp_q2')
        yield b.declareQueue(queue='mcs_amqp_q3')

        yield b.declareExchange(exchange='mcs_amqp_e12')

        yield b.bindQueue(exchange='mcs_amqp_e12', queue='mcs_amqp_q1')
        yield b.bindQueue(exchange='mcs_amqp_e12', queue='mcs_amqp_q2')


@zope.interface.implementer(amqp.IAMQPSchema)
class SingleQueueSchema(object):

    def __init__(self, queue, auto_delete=False):
        self.queue = queue
        self.auto_delete = auto_delete

    def declareSchema(self, b):
        return b.declareQueue(queue=self.queue, auto_delete=self.auto_delete)


@zope.interface.implementer(amqp.IAMQPSchema)
class SingleExchangeSchema(object):

    def __init__(self, exchange, auto_delete=False):
        self.exchange = exchange
        self.auto_delete = auto_delete

    def declareSchema(self, b):
        return b.declareExchange(
            exchange=self.exchange,
            auto_delete=self.auto_delete,
            exchange_type='fanout',
        )


# tests

class BaseTest(TestCase):

    schema = None

    def clientParams(self):
        return {
            'schema': self.schema,
            'host': "localhost",
            'port': 5672,
        }

    @defer.inlineCallbacks
    def setUp(self):
        params = self.clientParams()

        self.endpoint = endpoints.TCP4ClientEndpoint(reactor, "localhost", 5672)
        self.factory = amqp.AMQPFactory(**params)
        self.client = amqp.AMQPService(self.endpoint, self.factory, **params)
        self.client.startService()

        yield sleep(1)

        # yield self.client.notifyHandshaking()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.stopService()

    @defer.inlineCallbacks
    def clearQueue(self, queue, sleep_time=0.3):

        def dropmsg(m):
            logger.info("DROP MSG %r", m)

        sql = self.client.setupQueueConsuming(queue, dropmsg)
        yield sleep(sleep_time)
        yield sql.stopService()


class ReconnectTest(BaseTest):

    schema = SingleQueueSchema(QX, auto_delete=False)

    @defer.inlineCallbacks
    def rcv(self, x):
        logger.debug("RCV %r", x)
        self.messages_cnt += 1
        if self.publish:
            logger.debug("RESEND %r", x)
            yield self.client.publishMessage(
                exchange='',
                routing_key=QX,
                body=str(int(x) + 1),
                confirm=True,
            ).addErrback(lambda e: logger.info("ERR: %r", e))
        else:
            logger.debug("SKIP %r", x)

    @defer.inlineCallbacks
    def setUp(self):
        yield BaseTest.setUp(self)
        yield self.clearQueue(QX)
        self.messages_cnt = 0
        self.publish = True
        self.client.reconnect_max_delay = 0.01

    @defer.inlineCallbacks
    def clearTestingQueue(self):
        self.publish = False
        # consume & skip all messages
        yield sleep(0.1)
        self.messages_cnt = 0
        self.publish = True

    @defer.inlineCallbacks
    def consumeToken(self, sleep_time=0.3):
        logger.debug("start token consuming...")
        self.publish = False
        cnt = self.messages_cnt
        yield sleep(sleep_time)
        cnt = self.messages_cnt - cnt
        self.publish = True
        logger.debug("finish token consuming: %s", cnt)
        defer.returnValue(cnt)

    @defer.inlineCallbacks
    def pushToken(self):
        yield self.client.publishMessage(
            exchange='', routing_key=QX, body='0', confirm=True)

    @defer.inlineCallbacks
    def test_reconnection(self):

        sql = self.client.setupQueueConsuming(QX, self.rcv)
        yield self.clearQueue(QX)
        yield self.pushToken()

        for i in range(5):
            yield self.client.dropConnection()
            yield sleep(i / 10)

        cnt = yield self.consumeToken()
        yield sql.stopService()
        self.assertTrue(cnt > 0, "initial message was lost")

    @defer.inlineCallbacks
    def test_auto_disconnection(self):

        self.client.disconnect_delay = 0.5
        self.client.resetReconnectDelay()

        self.disconnections = 0

        def dropConnection():
            self.disconnections += 1
            old_dropConnection()

        old_dropConnection = self.client.dropConnection
        self.client.dropConnection = dropConnection

        sql = self.client.setupQueueConsuming(QX, self.rcv)

        yield self.pushToken()
        yield sleep(5)  # expect autodisconnects here

        # disable disconnects - we want to consume initial token
        self.client.disconnect_delay = None
        self.client.resetReconnectDelay()

        yield sleep(1)

        cnt = yield self.consumeToken(sleep_time=2)

        self.assertTrue(self.disconnections > 2, "auto disconnects was made")
        self.assertTrue(cnt > 0, "initial message was lost")

        yield sql.stopService()

    @defer.inlineCallbacks
    def test_reject_strategies(self):

        self.client.reconnect_max_delay = 0.01
        yield self.client.dropConnection()
        self.publish = True

        def rcv(x):
            self.messages_cnt += 1
            if self.publish:
                raise Exception("some error here")

        @defer.inlineCallbacks
        def doche(onerror):

            yield self.client.dropConnection()
            yield self.clearQueue(QX)
            self.messages_cnt = 0

            sql = self.client.setupQueueConsuming(
                QX, rcv, on_error=onerror, requeue_delay=0.1)

            self.publish = True
            yield self.client.publishMessage(
                exchange='', routing_key=QX, body=("***" + onerror))

            yield sleep(0.95)
            x1 = self.messages_cnt
            self.messages_cnt = 0

            yield self.client.dropConnection()
            yield sleep(0.95)
            x2 = self.messages_cnt
            self.messages_cnt = 0

            yield self.client.dropConnection()
            yield sleep(0.95)
            x3 = self.messages_cnt
            self.messages_cnt = 0

            yield sql.stopService()
            defer.returnValue((x1, x2, x3))

        x = yield doche('reject')
        self.assertEqual((1, 0, 0), x, "msg rejected")

        x = yield doche('requeue_once')
        self.assertEqual((2, 0, 0), x, "requeue once")

        x = yield doche('requeue_forever')
        self.assertEqual((9, 9, 9), x, "requeue forever")

        x = yield doche('requeue_hold')
        self.assertEqual((2, 1, 1), x, "msg holded")

        x = yield doche('do_nothing')
        self.assertEqual((1, 1, 1), x, "msg rejected")

        yield self.client.dropConnection()  # clear holded messages
        yield self.clearQueue(QX)
        self.messages_cnt = 0

    @defer.inlineCallbacks
    def test_invalid_handler(self):

        self.client.reconnect_max_delay = 0.01
        yield self.client.dropConnection()

        def rcv(x):
            self.messages_cnt += 1
            if self.publish:
                raise Exception("some error here")

        sql = self.client.setupQueueConsuming(
            QX, rcv,
            on_error='requeue_hold',
            requeue_delay=0.2,
        )
        yield sleep(0.1)

        self.publish = True
        yield self.client.publishMessage(exchange='', routing_key=QX, body="***")

        yield sleep(0.05)
        self.assertEqual(1, self.messages_cnt, "no redelivery yet")

        yield sleep(1.0)
        self.assertEqual(2, self.messages_cnt, "just 1 redelivery")

        yield self.client.dropConnection()
        yield sleep(1.0)
        self.assertEqual(3, self.messages_cnt, "more after disconnect")

        yield self.client.dropConnection()
        self.publish = False
        yield self.consumeToken()
        yield sql.stopService()

        self.assertEqual(4, self.messages_cnt, "message processed")

    @defer.inlineCallbacks
    def test_outgoing_queue(self):

        # TODO: duplication of 'test_reconnection', fix it
        self.client.reconnect_max_delay = 0.1
        processed_x = set()

        def rcv_unsafe(x):

            # some messages can be delivered several times - it's OK, just skip
            if x in processed_x:
                return
            processed_x.add(x)

            self.messages_cnt += 1
            if self.publish:
                self.client.publishMessage(
                    exchange='', routing_key=QX, body=str(int(x) + 1),
                    confirm=True,  # !!
                ).addBoth(lambda _: None)  # ignore result/errors

        sql = self.client.setupQueueConsuming(QX, rcv_unsafe)
        yield self.clearQueue(QX)

        yield self.client.publishMessage(exchange='', routing_key=QX, body='0')

        for i in range(5):
            yield self.client.dropConnection()
            yield sleep(i / 10)

        cnt = yield self.consumeToken()
        yield sql.stopService()
        self.assertTrue(cnt > 0, "initial message was lost")


class QueueConsumerTest(BaseTest):

    schema = SingleQueueSchema(Q1)

    @defer.inlineCallbacks
    def setUp(self):
        yield BaseTest.setUp(self)
        yield self.clearQueue(Q1)

    @defer.inlineCallbacks
    def test_several_messages_without_serialization(self):
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm1")
        result = []
        sql = self.client.setupQueueConsuming(Q1, result.append)
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm2")
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm3")
        yield sleep(0.1)
        yield sql.stopService()
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm_after")
        yield sleep(0.1)
        self.assertEqual(result, ["tm1", "tm2", "tm3"])
        del result[:]
        yield sql.startService()
        yield sleep(0.2)
        self.assertEqual(result, ["tm_after"])

    @defer.inlineCallbacks
    def send_and_receive_one_message(self, content_type, message_body):

        yield self.clearQueue(Q1)
        result_d = defer.Deferred()

        def on_msg(m):
            result_d.callback(m)

        sql = self.client.setupQueueConsuming(Q1, on_msg, no_ack=1)

        yield self.client.publishMessage(
            exchange='', routing_key=Q1,
            body=message_body, content_type=content_type)

        result = yield result_d
        self.assertEqual(message_body, result)

        yield sql.stopService()

    @defer.inlineCallbacks
    def test_serialization(self):
        message_body = {'one': 1, 'list': [1, {'sub': ['a', 'b']}, 3], 'bool': False}
        yield self.send_and_receive_one_message('json', message_body)
        yield self.send_and_receive_one_message('msgpack', message_body)
        yield self.send_and_receive_one_message('application/json', message_body)
        yield self.send_and_receive_one_message('application/msgpack', message_body)

    @defer.inlineCallbacks
    def test_without_serialization(self):
        yield self.send_and_receive_one_message(None, "message1")
        yield self.send_and_receive_one_message('', "message2")
        yield self.send_and_receive_one_message('plain/text', "message2")

    @defer.inlineCallbacks
    def test_publish_to_wrong_queue(self):

        try:
            yield self.client.publishMessage(
                exchange='invalid-exchange-name',
                routing_key="rkrk",
                body="xxx",
            )
        except Exception:
            pass
        else:
            self.fail()

        yield self.send_and_receive_one_message('', "message2")

    @defer.inlineCallbacks
    def test_consume_raw_message(self):

        result_d = defer.Deferred()

        sql = self.client.setupQueueConsuming(
            Q1, result_d.callback, deserialize=False, no_ack=1)

        yield self.client.publishMessage(
            exchange='', routing_key=Q1,
            body="BODY", content_type='plain/text')

        msg = yield result_d

        self.assertEqual("BODY", msg.body)
        self.assertEqual('plain/text', msg.content_type)
        self.assertFalse(msg.redelivered)
        self.assertEqual(Q1, msg.routing_key)
        self.assertEqual('', msg.exchange)

        yield sql.stopService()

    @defer.inlineCallbacks
    def test_parallel_consumers(self):

        results = []

        def on_msg(m):
            results.append(m)
            return sleep(0.2)

        # process in 10 "threads"
        sql = self.client.setupQueueConsuming(Q1, on_msg, parallel=1)
        yield sql.stopService()

        self.assertEqual([], results)

        for i in range(5):
            self.client.publishMessage(
                exchange='', routing_key=Q1, confirm=0,
                body=str(i), content_type='plain/text')

        # each message takes 0.2s, 10 threads give us 20 processed messages
        yield sql.startService()
        yield sleep(0.3)
        self.assertEqual(2, len(results), "should recieve exact 20 messages")

        yield sql.stopService()
        del results[:]
        yield sleep(0.5)
        self.assertEqual(0, len(results), "no more messages (consuming cancelled)")

    @defer.inlineCallbacks
    def test_ping_pong(self):

        messages_cnt = [0]

        def rcv(x):
            messages_cnt[0] += 1
            return self.client.publishMessage(
                exchange='', routing_key=Q1, body=str(int(x) + 1))

        sql = self.client.setupQueueConsuming(Q1, rcv, no_ack=True, parallel=10)
        for i in range(10):
            yield self.client.publishMessage(exchange='', routing_key=Q1, body="0")

        for i in range(20):
            yield sleep(0.1)
            self.assertTrue(messages_cnt[0] > 10, "greater than 100 messages per second")
            messages_cnt[0] = 0

        yield sql.stopService()
        yield self.clearQueue(Q1)

    @defer.inlineCallbacks
    def test_quick_consume_and_cancel(self):
        sql = self.client.setupQueueConsuming(Q1, lambda _: None)
        yield sql.stopService()
        for _ in range(20):
            yield defer.maybeDeferred(sql.startService)
            yield defer.maybeDeferred(sql.stopService)


class ExchangeConsumerTest(BaseTest):

    schema = SingleExchangeSchema(E1)

    @defer.inlineCallbacks
    def test_send_to_exchange(self):

        result = []
        sels = [self.client.setupExchangeConsuming(E1, result.append) for _ in range(5)]

        yield sleep(0.2)
        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm1")
        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm2")
        yield sleep(0.2)

        for sel in sels:
            yield sel.stopService()

        yield sleep(0.2)
        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm_after")
        yield sleep(1)

        self.assertEqual(
            sorted(["tm1", "tm2"] * len(sels)),
            sorted(result),
        )
        del result[:]

        for sel in sels:
            yield sel.startService()

        yield sleep(0.2)
        self.assertEqual([], result)

        for sel in sels:
            yield sel.stopService()
