# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.internet import reactor, protocol
from twisted.internet import defer
from twisted.trial.unittest import TestCase
from twisted.web.test.requesthelper import DummyRequest
from twisted.web.client import Agent, readBody
from twisted.web.iweb import UNKNOWN_LENGTH

from twoost import web, timed


class MyCustomError(Exception):
    pass


class BadHttpDownloader(protocol.Protocol):

    def dataReceived(self, data):
        self.transport.loseConnection()

    def connectionLost(self, reason):
        assert False


class MyDummyRequest(DummyRequest):

    startedWriting = False
    method = b'GET'

    def write(self, method, data):
        DummyRequest.write(self, data)
        self.startedWriting = True


class TestResource(web.Resource):

    ifLeaf = True

    def getChild(self, path, request):
        assert path == ''
        return self

    def render_GET(self, request):
        testn = request.args['testn'][0]
        return getattr(self, 'render_GET_' + testn)(request)

    def render_POST(self, request):
        testn = request.args['testn'][0]
        return getattr(self, 'render_POST_' + testn)(request)

    def render_GET_nodeferred(self, request):
        return 'ok'

    @defer.inlineCallbacks
    def render_GET_deferred(self, request):
        yield timed.sleep(0.01)
        defer.returnValue('ok')

    @defer.inlineCallbacks
    def render_GET_write_with_deferred(self, request):
        request.write('o')
        yield timed.sleep(0.01)
        request.write('k')

    def render_GET_failure_without_deferred(self, request):
        raise MyCustomError("error")

    @defer.inlineCallbacks
    def render_GET_failure_with_deferred(self, request):
        yield timed.sleep(0.01)
        raise MyCustomError("error")

    @defer.inlineCallbacks
    def render_POST_slow(self, request):
        for i in range(1000):
            request.write("TeesT" * 10)
            yield timed.sleep(0.01)

    def render_GET_canceller(self, request):
        def canceller(_):
            request.write("cancelled")
        d = defer.Deferred(canceller=canceller)
        return timed.sleep(0.1).addCallback(lambda _: d)

    def render_GET_nocanceller(self, request):
        return timed.sleep(0.1)


# ---

class DeferredResourceTest(TestCase):

    def setUp(self):
        self.agent = Agent(reactor)
        self.resource = TestResource()
        self.site = web.UnitTestSite(
            self.resource,
            displayTracebacks=True,  # we'll check traceback
        )
        self.listening_port = reactor.listenTCP(0, self.site)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.listening_port.stopListening()
        yield self.site.loseAllConnections()

    def request_url(self, testn):
        return "http://localhost:{0}/?testn={1}".format(
            self.listening_port.getHost().port,
            testn,
        )

    @defer.inlineCallbacks
    def check_ok_body(self, resp):
        body = yield readBody(resp)
        self.assertEqual(200, resp.code)
        self.assertEqual('ok', body)

    # --- tests

    @defer.inlineCallbacks
    def test_nodeferred(self):
        resp = yield self.agent.request('GET', self.request_url('nodeferred'))
        self.assertEqual(2, resp.length)
        yield self.check_ok_body(resp)

    @defer.inlineCallbacks
    def test_deferred(self):
        resp = yield self.agent.request('GET', self.request_url('deferred'))
        self.assertEqual(2, resp.length)
        yield self.check_ok_body(resp)

    @defer.inlineCallbacks
    def test_write_with_deferred(self):
        resp = yield self.agent.request('GET', self.request_url('write_with_deferred'))
        self.assertEqual(UNKNOWN_LENGTH, resp.length)
        yield self.check_ok_body(resp)

    @defer.inlineCallbacks
    def test_slow_post(self):
        resp = yield self.agent.request('POST', self.request_url('slow'))
        self.assertEqual(UNKNOWN_LENGTH, resp.length)
        body = yield readBody(resp)
        self.assertEqual(200, resp.code)
        self.assertEqual("TeesT" * 10000, body)

    @defer.inlineCallbacks
    def test_failure_without_deferred(self):

        resp = yield self.agent.request('GET', self.request_url('failure_without_deferred'))
        self.assertEqual(500, resp.code)

        body = yield readBody(resp)
        self.assertTrue('raise MyCustomError("error")' in body)

        errs = self.flushLoggedErrors()
        self.assertEqual(1, len(errs))
        errs[0].trap(MyCustomError)

    @defer.inlineCallbacks
    def test_failure_with_deferred(self):

        resp = yield self.agent.request('GET', self.request_url('failure_with_deferred'))
        self.assertEqual(500, resp.code)

        body = yield readBody(resp)
        self.assertTrue('raise MyCustomError("error")' in body)

        errs = self.flushLoggedErrors()
        self.assertEqual(1, len(errs))
        errs[0].trap(MyCustomError)

    # TODO: tests with cancellation!


class RoutingTest(TestCase):

    def _mk_ress(self, n):
        return [web.Resource() for _ in range(n)]

    def assertChild(self, expected_res, root, path):
        for p in path:
            self.assertIn(p, root.children)
            root = root.children[p]
        self.assertIs(expected_res, root)

    def test_only_root(self):
        res = web.Resource()
        r = web.buildResourceTree({'': res})
        self.assertIs(res, r)
        self.assertNot(r.children)

    def test_asis(self):
        res = web._PingResource()
        r = web.buildResourceTree(res)
        self.assertIs(res, r)
        self.assertNot(r.children)

    def test_subresourecs(self):
        r1, r2, r3, r4 = self._mk_ress(4)
        r = web.buildResourceTree([
            ("a/b", r1),
            ("a", r2),
            ("c/d/", r3),
            ("", r4),
        ])
        self.assertChild(r1, r, ['a', 'b'])
        self.assertChild(r2, r, ['a'])
        self.assertChild(r3, r, ['c', 'd', ''])
        self.assertChild(r4, r, [])

    def test_recurive(self):
        r1, r2 = self._mk_ress(2)
        r = web.buildResourceTree({
            'a': {'b': {'c': r1}},
            'a/b': r2,
        })
        self.assertChild(r1, r, ['a', 'b', 'c'])
        self.assertChild(r2, r, ['a', 'b'])

    def test_dublicates(self):
        r1, r2 = self._mk_ress(2)
        try:
            web.buildResourceTree({
                'a': {'b': r1},
                'a/b': r2,
            })
        except ValueError:
            pass
        else:
            self.fail("expected ValueError")
