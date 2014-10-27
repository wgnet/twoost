# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.internet import reactor, defer
from twisted.trial.unittest import TestCase

from twoost import web, httprpc, timed
from twoost._misc import required_attr


class RPCTestAbstract(object):

    rpc_resource_class = required_attr
    rpc_proxy_class = required_attr
    rpc_method_prefix = required_attr

    def setUp(self):

        self.rpcres = self.rpc_resource_class({
            'ping': lambda: 'pong',
            'uri': httprpc.withRequest(lambda req: req.uri),
            'sum': lambda *args: sum(args),
            'sleep': self._sleep
        })
        setattr(self.rpcres, self.rpc_method_prefix + 'echo', lambda x: x)

        self.site = web.UnitTestSite(self.rpcres)
        self.listening_port = reactor.listenTCP(0, self.site)

        port = self.listening_port.getHost().port
        self.server_url = "http://localhost:" + str(port)
        self.proxy = self.rpc_proxy_class(url=self.server_url, timeout=0.1)

        self._sleeps = []

    def _sleep(self, t):
        d = timed.sleep(t)
        d.addErrback(lambda f: f.trap(defer.CancelledError) and None)
        self._sleeps.append(d)
        return d

    @defer.inlineCallbacks
    def tearDown(self):
        for s in self._sleeps:
            s.cancel()
        yield self.listening_port.stopListening()
        # yield self.site.loseAllConnections()

    @defer.inlineCallbacks
    def test_ping_pong(self):
        d = yield self.proxy.callRemote('ping')
        self.assertEqual('pong', d)

    @defer.inlineCallbacks
    def test_with_uri(self):
        d = yield self.proxy.callRemote('uri')
        self.assertTrue(d)

    @defer.inlineCallbacks
    def test_multi_args(self):
        d = yield self.proxy.callRemote('sum', 1, 2, 3)
        self.assertEqual(6, d)
        d = yield self.proxy.callRemote('sum', 1, 2, 3, 4, 5)
        self.assertEqual(15, d)

    @defer.inlineCallbacks
    def test_serialize_dererizlize(self):
        payload = {
            'list': [1, 2, 3],
            'int': 21000000,
            'float': 12.25,
            'bool': True,
            'none': None,
            'unicode': u"русский текст",
            'dict': {'one': 1, 'two': 2},
        }
        payload2 = yield self.proxy.callRemote('echo', payload)
        self.assertEqual(payload, payload2)

    @defer.inlineCallbacks
    def test_timeout(self):
        try:
            yield self.proxy.callRemote('sleep', 0.5)
        except defer.CancelledError:
            pass
        else:
            self.fail("expected CancelledError")


class DumbRPCTest(RPCTestAbstract, TestCase):
    rpc_resource_class = httprpc.DumbRPCResource
    rpc_proxy_class = httprpc.DumbRPCProxy
    rpc_method_prefix = 'dumbrpc_'


class XMLRPCTest(RPCTestAbstract, TestCase):
    rpc_resource_class = httprpc.XMLRPCResource
    rpc_proxy_class = httprpc.XMLRPCProxy
    rpc_method_prefix = 'xmlrpc_'
