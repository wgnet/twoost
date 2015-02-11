# coding: utf-8

from __future__ import print_function, division, absolute_import

import json
import cgi
import xmlrpclib
import base64

import zope.interface

from twisted.web.http_headers import Headers
from twisted.web import client, xmlrpc
from twisted.internet import defer, reactor
from twisted.python import reflect

from twoost import web, timed


import logging
logger = logging.getLogger(__name__)


__all__ = [
    'XMLRPCResource',
    'XMLRPCProxy',
    'DumbRPCProxy',
    'DumbRPCResource',
    'HttpRPCError',
    'withRequest',
]


# same for DumbRPC
withRequest = xmlrpc.withRequest


def _log_method_call(fn, method):
    if fn is None:
        return
    if getattr(fn, 'withRequest', False):
        @withRequest
        def wrapper(request, *args):
            logger.debug("invoked method %r with args %r", method, args)
            return fn(request, *args)
    else:
        def wrapper(*args):
            logger.debug("invoked method %r with args %r", method, args)
            return fn(*args)
    return wrapper


@zope.interface.implementer(client.IAgent)
class BasicAuthAgent(object):
    """
    An L{Agent} wrapper to handle basic auth.
    """

    def __init__(self, agent, username, password):
        self._agent = agent
        self._auth_header = b"Basic " + base64.b64encode("%s:%s" % (username, password))

    def request(self, method, uri, headers=None, bodyProducer=None):
        headers = Headers() if headers is None else headers.copy()
        headers.addRawHeader(b'authorization', self._auth_header)
        return self._agent.request(method, uri, headers, bodyProducer)


# -- dumbrpc

class HttpRPCError(Exception):

    def __init__(self, response_code, response_body, response_content_type=None):
        Exception.__init__(self, response_code, response_body, response_content_type)
        self.response_code = response_code or 0
        self.response_body = response_body or ""
        self.response_content_type = response_content_type


class DumbRPCResource(web.LeafResourceMixin, web.Resource):

    def __init__(self, methods=None):
        web.Resource.__init__(self)
        self._methods = dict(methods or {})

    def render_GET(self, request):
        return "methods: " + "\n".join(self.listProcedures())

    def lookupProcedure(self, method):
        if method in self._methods:
            f = self._methods[method]
        else:
            f = getattr(self, "dumbrpc_%s" % method, None)
        return _log_method_call(f, method)

    def listProcedures(self):
        a = set(self._methods)
        b = set(reflect.prefixedMethodNames(self.__class__, 'dumbrpc_'))
        return sorted(a | b)

    def _decodeRequestBody(self, request):

        ctype = (request.requestHeaders.getRawHeaders(b'content-type') or [None])[0]
        body = request.content.read()
        request.content.seek(0, 0)

        key, _ = cgi.parse_header(ctype)
        if ctype == b'application/json':
            args = json.loads(body.decode('utf-8'))
        else:
            raise ValueError("expected conten-type is 'application/json'")

        return args

    @defer.inlineCallbacks
    def render_POST(self, request):

        method = request.args.get('method', [None])[0]

        callback = self.lookupProcedure(method) if method else None
        if callback is None:
            request.setResponseCode(404)
            defer.returnValue("no method %r" % method)

        try:
            args = self._decodeRequestBody(request)
        except ValueError as e:
            request.setResponseCode(406)  # not acceptable
            defer.returnValue(str(e))

        if not isinstance(args, list):
            args = [args]
        if getattr(callback, 'withRequest', False):
            args = [request] + args

        logger.debug("callRemote %r with args %r", method, args)

        res = yield defer.maybeDeferred(callback, *args)
        resp_body = json.dumps(res)
        request.setHeader(b'content-type', b'application/json')

        logger.debug("result is %r", resp_body)
        defer.returnValue(resp_body)


class DumbRPCProxy(object):

    def __init__(self, url, agent=None, timeout=60.0):
        assert url
        self.url = url
        self.timeout = timeout
        self.agent = agent or client.Agent(reactor)
        self.callRemote = timed.withTimeout(self.timeout)(self._call_remote)

    @defer.inlineCallbacks
    def _call_remote(self, method, *args):

        logger.debug("remote call to %r, method %r with args %r", self.url, method, args)

        body = json.dumps(args).encode('utf-8')
        uri = self.url + "?method=" + method

        body_p = web.StringBodyProducer(body)
        headers = Headers({b'content-type': [b'application/json']})

        logger.debug("call request to %r, args %r", uri, args)
        resp = yield self.agent.request(
            'POST', uri, headers=headers, bodyProducer=body_p)
        logger.debug("response code %r from %r", resp.code, uri)

        resp_ct = resp.headers.getRawHeaders(b'content-type', [None])[-1]
        resp_body = yield client.readBody(resp)

        if resp.code != 200:
            raise HttpRPCError(resp.code, resp_body, resp_ct)

        # TODO: read body & parse errors
        if not resp_body:
            raise HttpRPCError(resp.code, resp_body, response_content_type=resp_ct)

        response = json.loads(resp_body)
        defer.returnValue(response)


# --- xml-rpc

class XMLRPCResource(xmlrpc.XMLRPC):

    def __init__(self, methods=None, allowNone=True, useDateTime=False):
        xmlrpc.XMLRPC.__init__(self, allowNone=True, useDateTime=False)
        self._methods = methods or {}

    def lookupProcedure(self, method):
        if method in self._methods:
            f = self._methods[method]
        else:
            f = xmlrpc.XMLRPC.lookupProcedure(self, method)
        return _log_method_call(f, method)

    def listProcedures(self):
        a = set(self._methods)
        b = set(xmlrpc.XMLRPC.listProcedures(self))
        return sorted(a | b)


class XMLRPCProxy(object):

    def __init__(self, url, agent=None, timeout=60,
                 xmlrpclib_use_datetime=False, xmlrpclib_allow_none=True):
        self.url = url
        self.timeout = timeout
        self.agent = agent or client.Agent(reactor)
        self.callRemote = timed.withTimeout(self.timeout)(self._call_remote)
        self.xmlrpclib_allow_none = xmlrpclib_allow_none
        self.xmlrpclib_use_datetime = xmlrpclib_use_datetime

    @defer.inlineCallbacks
    def _call_remote(self, method, *args):

        body = xmlrpc.payloadTemplate % (
            method,
            xmlrpclib.dumps(args, allow_none=self.xmlrpclib_allow_none),
        )
        body_p = web.StringBodyProducer(body)
        headers = Headers({
            b'content-type': ['text/xml'],
            b'content-length': [str(len(body))],
        })
        logger.debug("call request to %r, args %r", self.url, args)
        resp = yield self.agent.request(
            'POST', self.url, headers=headers, bodyProducer=body_p)
        logger.debug("response code %r", resp.code)

        resp_ct = resp.headers.getRawHeaders(b'content-type', [None])[-1]
        resp_body = yield client.readBody(resp)

        if resp.code != 200:
            raise HttpRPCError(resp.code, resp_body, resp_ct)

        # TODO: read body & parse errors
        if not resp_body:
            raise HttpRPCError(resp.code, None, resp_body, response_content_type=resp_ct)

        response = xmlrpclib.loads(resp_body, use_datetime=self.xmlrpclib_use_datetime)
        defer.returnValue(response[0][0])
