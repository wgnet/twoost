# coding: utf-8

from __future__ import print_function, division, absolute_import

import time

from twisted.internet import reactor
from twisted.internet import defer
from twisted.trial.unittest import TestCase
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers
from twisted.web.http import datetimeToString

from twoost import web, authhmac


class TestResource(web.LeafResourceMixin, web.Resource):

    def render_GET(self, request):
        return 'ok'

    def render_POST(self, request):
        return 'ok'


class AuthHMACTest(TestCase):

    def setUp(self):
        self.resource = authhmac.protectResource(TestResource(), "key", "secret")
        self.listening_port = reactor.listenTCP(0, web.UnitTestSite(self.resource))
        self.agent = Agent(reactor)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.listening_port.stopListening()

    def request_url(self, rest):
        return "http://localhost:{0}/{1}".format(self.listening_port.getHost().port, rest)

    @defer.inlineCallbacks
    def check_ok(self, resp):
        body = yield readBody(resp)
        self.assertEqual(200, resp.code)
        self.assertEqual('ok', body)

    def check_fail(self, resp):
        self.assertEqual(401, resp.code)
        return defer.succeed(None)

    # --- tests

    @defer.inlineCallbacks
    def test_ok_get_simple(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        resp = yield agent.request('GET', self.request_url(""), None, None)
        yield self.check_ok(resp)

    @defer.inlineCallbacks
    def test_ok_get(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        for p in [
                "//path/path//",
                "?a=1&b=2",
                "?",
                "#",
                "#test test test",
                "?a=1&b=2#",
                "$..$Ssrt?rsat=sart325ea..rst2%%sta#srat=/../#xxxyyy",
        ]:
            resp = yield agent.request('GET', self.request_url(p), None, None)
            yield self.check_ok(resp)

    @defer.inlineCallbacks
    def test_wrong_key(self):
        agent = authhmac.AuthHMACAgent(self.agent, "bad-key", "secret")
        resp = yield agent.request('GET', self.request_url(""), None, None)
        yield self.check_fail(resp)

    @defer.inlineCallbacks
    def test_too_old_request(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        headers = Headers({'date': [datetimeToString(time.time() - 1500)]})
        bodyp = web.StringBodyProducer("some content")
        resp = yield agent.request('POST', self.request_url(""), headers, bodyp)
        yield self.check_fail(resp)

    @defer.inlineCallbacks
    def test_wrong_secret(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "bad-secret")
        resp = yield agent.request('GET', self.request_url(""), None, None)
        yield self.check_fail(resp)

    @defer.inlineCallbacks
    def test_ok_post(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        bodyp = web.StringBodyProducer("some content")
        resp = yield agent.request('POST', self.request_url(""), None, bodyp)
        yield self.check_ok(resp)

    @defer.inlineCallbacks
    def test_ok_post_with_ct(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        bodyp = web.StringBodyProducer("some content")
        headers = Headers({'content-type': ['unknown-content-type']})
        resp = yield agent.request('POST', self.request_url(""), headers, bodyp)
        yield self.check_ok(resp)

    @defer.inlineCallbacks
    def test_wrong_md5(self):
        agent = authhmac.AuthHMACAgent(self.agent, "key", "secret")
        headers = Headers({'content-md5': ["xxx"]})
        bodyp = web.StringBodyProducer("some content")
        resp = yield agent.request('POST', self.request_url(""), headers, bodyp)
        yield self.check_fail(resp)
