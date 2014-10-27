# coding: utf-8

from __future__ import print_function, division, absolute_import

import uuid

from binascii import crc32

from twisted.internet import defer, error
from twisted.application.service import Application, IService
from twisted.trial.unittest import TestCase

from twoost import memcache, app, conf, timed, pclient


def gr(n):
    return [
        uuid.uuid4().get_hex()
        for _ in range(n)
    ]


class MemcacheTestCase(TestCase):

    @defer.inlineCallbacks
    def setUp(self):

        self.config = {
            'MEMCACHE_SERVERS': {
                'c0': {'host': 'localhost'},
                'c1': {'host': 'localhost'},
                'c2': {'host': 'localhost'},
            },
        }
        conf.settings.add_config(self.config)
        self.app = Application(__name__)
        self.memcache = app.build_memcache(self.app)
        IService(self.app).startService()

        yield timed.sleep(1)

    @staticmethod
    def clientByKey(key):
        return "c%d" % (crc32(key) % 3)

    @defer.inlineCallbacks
    def tearDown(self):
        yield IService(self.app).stopService()
        conf.settings.remove_config(self.config)

    @defer.inlineCallbacks
    def test_set_get(self):
        key, val = gr(2)
        yield self.memcache['c1'].set(key, val, flags=123)
        f, v = yield self.memcache['c1'].get(key)
        self.assertEqual(123, f)
        self.assertEqual(val, v)

    @defer.inlineCallbacks
    def test_get_multi(self):
        k1, k2, k3 = gr(3)
        v1, v2, v3 = gr(3)
        c = self.memcache['c1']
        yield c.set(k1, v1)
        yield c.set(k3, v3)
        vs = yield c.getMultiple([k1, k2, k3])
        self.assertEqual({k1: (0, v1), k2: (0, None), k3: (0, v3)}, vs)

    @defer.inlineCallbacks
    def test_reconnect(self):
        key, val = gr(2)
        c = self.memcache['c1']
        yield c.set(key, val)

        c.scheduledDisconnect()
        try:
            yield c.get(key, val)
        except error.ConnectionClosed:
            pass
        else:
            self.fail("expected ConnectionClosed")

        yield timed.sleep(0.3)

        fv = yield c.get(key)
        self.assertEqual((0, val), fv)

    @defer.inlineCallbacks
    def test_multi_client(self):
        m = self.memcache.multiClient(self.clientByKey)
        keys = gr(50)
        expected_vs = {}
        for i, key in enumerate(keys):
            yield m.set(key, str(i))
            expected_vs[key] = 0, str(i)

        self.memcache['c1'].scheduledDisconnect()

        try:
            yield m.getMultiple(keys, ignoreErrors=False)
        except defer.FirstError as e:
            self.assertTrue(e.subFailure.check(error.ConnectionClosed))
        else:
            self.fail("expected ConnectionClosed")

        vs0 = yield m.getMultiple(keys)
        yield timed.sleep(0.3)
        self.assertTrue(all(
            isinstance(e.value, error.ConnectionClosed)
            or (isinstance(e.value, RuntimeError) and "not connected" == str(e.value))
            for e in self.flushLoggedErrors()
        ))

        vs = yield m.getMultiple(keys)
        self.assertEqual(expected_vs, vs)
        self.assertTrue(vs0)
        self.assertTrue(len(vs0) < len(vs))
