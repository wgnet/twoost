# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.internet import reactor
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


class SingleQueueSchema(object):

    def __init__(self, queue, auto_delete=False):
        self.queue = queue
        self.auto_delete = auto_delete

    def declareSchema(self, b):
        return b.declareQueue(queue=self.queue, auto_delete=self.auto_delete)


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
        return dict(schema=self.schema)

    @defer.inlineCallbacks
    def setUp(self):
        defer.setDebugging(True)
        self.client = amqp.AMQPClient(**self.clientParams())
        self.client_connector = reactor.connectTCP("localhost", 5672, self.client)
        yield self.client.notifyHandshaking()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.disconnectAndWait(self.client_connector)

    @defer.inlineCallbacks
    def clearQueue(self, queue, sleep_time=0.3):
        sql = amqp.QueueConsumer(self.client, queue, lambda _: None)
        yield sql.startService()
        yield sleep(sleep_time)
        yield sql.stopService()


class ReconnectTest(BaseTest):

    schema = SingleQueueSchema(QX, auto_delete=False)

    def rcv(self, x):
        logger.debug("RCV %r", x)
        self.messages_cnt += 1
        if self.publish:
            logger.debug("RESEND %r", x)
            self.client.publishMessage(
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
        self.client.maxDelay = 0.01

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
        yield self.client.publishMessage(exchange='', routing_key=QX, body='0', confirm=True)

    @defer.inlineCallbacks
    def test_reconnection(self):

        sql = amqp.QueueConsumer(self.client, QX, self.rcv)
        yield sql.startService()
        yield self.clearQueue(QX)
        yield self.pushToken()

        for i in range(5):
            yield self.client_connector.disconnect()
            yield sleep(i / 10)

        cnt = yield self.consumeToken()
        yield sql.stopService()
        self.assertTrue(cnt > 0, "initial message was lost")

    @defer.inlineCallbacks
    def test_auto_disconnection(self):

        self.client.maxDelay = 0.01
        self.client.disconnectDelay = 0.3
        self.client.resetDelay()

        self.disconnections = 0

        def scheduledDisconnect():
            self.disconnections += 1
            old_scheduledDisconnect()

        old_scheduledDisconnect = self.client.scheduledDisconnect
        self.client.scheduledDisconnect = scheduledDisconnect

        sql = amqp.QueueConsumer(self.client, QX, self.rcv)
        yield sql.startService()
        yield self.pushToken()
        yield sleep(4)  # expect autodisconnects here

        # disable disconnects - we want to consume initial token
        self.client.scheduledDisconnect = lambda: None

        cnt = yield self.consumeToken(sleep_time=2)

        self.assertTrue(self.disconnections > 1, "auto disconnections was made")
        self.assertTrue(cnt > 0, "initial message was lost")

        yield sql.stopService()

    @defer.inlineCallbacks
    def test_invalid_handler(self):

        self.client.maxDelay = 0.01
        yield self.client_connector.disconnect()

        def rcv(x):
            self.messages_cnt += 1
            if self.publish:
                raise Exception("some error here")

        sql = amqp.QueueConsumer(
            self.client, QX, rcv,
            message_reqeue_delay=0.2,
            hang_rejected_messages=True,
        )
        yield sleep(0.1)

        yield sql.startService()

        self.publish = True
        yield self.client.publishMessage(exchange='', routing_key=QX, body="***")

        yield sleep(0.05)
        self.assertEqual(1, self.messages_cnt, "no redelivery yet")

        yield sleep(1.0)
        self.assertEqual(2, self.messages_cnt, "just 1 redelivery")

        self.client_connector.disconnect()
        yield sleep(1.0)
        self.assertEqual(3, self.messages_cnt, "more after disconnect")

        yield self.client_connector.disconnect()
        self.publish = False
        yield self.consumeToken()
        yield sql.stopService()

        self.assertEqual(4, self.messages_cnt, "message processed")

    @defer.inlineCallbacks
    def test_outgoing_queue(self):

        # TODO: duplication of 'test_reconnection', fix it
        self.client.maxDelay = 0.1
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

        sql = amqp.QueueConsumer(self.client, QX, rcv_unsafe)
        yield sql.startService()
        yield self.clearQueue(QX)

        yield self.client.publishMessage(exchange='', routing_key=QX, body='0')

        for i in range(5):
            yield self.client_connector.disconnect()
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
        sql = amqp.QueueConsumer(self.client, Q1, result.append)
        yield sql.startService()
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm2")
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm3")
        yield sleep(0.1)
        yield sql.stopService()
        yield self.client.publishMessage(exchange='', routing_key=Q1, body="tm_after")
        yield sleep(0.1)
        self.assertEqual(result, ["tm1", "tm2", "tm3"])
        del result[:]
        yield sql.startService()
        yield sleep(0.1)
        self.assertEqual(result, ["tm_after"])

    @defer.inlineCallbacks
    def send_and_receive_one_message(self, content_type, message_body):

        result_d = defer.Deferred()

        def on_msg(m):
            result_d.callback(m)
            return sql.stopService()

        sql = amqp.QueueConsumer(self.client, Q1, on_msg)

        yield sql.startService()

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
        yield self.send_and_receive_one_message('pickle', message_body)
        yield self.send_and_receive_one_message('application/json', message_body)
        yield self.send_and_receive_one_message('application/pickle', message_body)

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

        def on_msg(m):
            result_d.callback(m)
            return sql.stopService()

        sql = amqp.QueueConsumer(self.client, Q1, on_msg, deserialize=False)
        yield sql.startService()
        yield self.client.publishMessage(
            exchange='', routing_key=Q1,
            body="BODY", content_type='plain/text')

        msg = yield result_d

        self.assertEqual("BODY", msg.body)
        self.assertEqual('plain/text', msg.content_type)
        self.assertFalse(msg.redelivered)
        self.assertEqual(Q1, msg.routing_key)
        self.assertEqual('', msg.exchange)

    @defer.inlineCallbacks
    def test_parallel_consumers(self):

        results = []

        def on_msg(m):
            results.append(m)
            return sleep(0.2)

        # process in 10 "threads"
        sql = amqp.QueueConsumer(self.client, Q1, on_msg, parallel=10)

        for i in range(50):
            yield self.client.publishMessage(
                exchange='', routing_key=Q1,
                body=str(i), content_type='plain/text')

        yield sql.startService()
        # each message takes 0.2s, 10 threads give us 20 processed messages
        yield sleep(0.3)
        self.assertEqual(20, len(results), "should recieve exact 20 messages")
        del results[:]

        yield sql.stopService()
        yield sleep(1)
        self.assertEqual(0, len(results), "no more messages (consuming cancelled)")

    @defer.inlineCallbacks
    def test_ping_pong(self):

        messages_cnt = [0]

        def rcv(x):
            messages_cnt[0] += 1
            return self.client.publishMessage(
                exchange='', routing_key=Q1, body=str(int(x) + 1))

        sql = amqp.QueueConsumer(self.client, Q1, rcv, no_ack=True, parallel=10)
        yield sql.startService()
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
        sql = amqp.QueueConsumer(self.client, Q1, lambda _: None)
        for _ in range(20):
            yield sql.startService()
            yield sql.stopService()


class ExchangeConsumerTest(BaseTest):

    schema = SingleExchangeSchema(E1)

    @defer.inlineCallbacks
    def test_send_to_exchange(self):

        result = []
        sels = [amqp.ExchangeConsumer(self.client, E1, result.append) for _ in range(5)]
        for sel in sels:
            yield sel.startService()

        yield sleep(0.2)
        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm1")
        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm2")
        yield sleep(0.2)

        for sel in sels:
            yield sel.stopService()

        yield self.client.publishMessage(exchange=E1, routing_key=Q1, body="tm_after")
        yield sleep(0.2)

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
