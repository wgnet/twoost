# coding: utf-8

from __future__ import print_function, division, absolute_import

import weakref
import itertools
import collections

from StringIO import StringIO

import zope.interface

from twisted.internet import defer, reactor
from twisted.internet.interfaces import IConsumer

from twisted.web.resource import IResource, ErrorPage, getChildForRequest
from twisted.web.resource import Resource as _Resource
from twisted.web.server import NOT_DONE_YET, Site as _Site
from twisted.web.client import FileBodyProducer
from twisted.web.error import UnsupportedMethod
from twisted.web.wsgi import WSGIResource as _WSGIResource
from twisted.python import log
from twisted.python.compat import intToBytes

from twoost.conf import settings
from twoost._misc import lazycol


import logging
logger = logging.getLogger(__name__)


__all__ = [
    'Resource',
    'Site',
    'UnitTestSite',
    'LeafResourceMixin',
    'MergedResource',
    'DynamicResource',
    'WSGIResource',
    'StringBodyProducer',
    'StringConsumer',
    'buildSite',
    'buildResourceTree',
]


# -- various resources


class Resource(_Resource, object):

    """Hacked version of Resource with support of returning `Deferreds` from `render_*`"""

    def _maybeSetContentLengthHeader(self, request, length):
        if request.responseHeaders.getRawHeaders(b'content-length'):
            # 'content-length' has been set manually
            return
        # twisted skip new headers when 'self.write' already has been callled
        request.setHeader(b'content-length', intToBytes(length))

    def finishRequest(self, request, body=None):
        if body:
            self._maybeSetContentLengthHeader(request, len(body))
            request.write(body)
        if not request.__finished:
            request.finish()

    def failRequest(self, request, reason):

        logger.debug("fail request %r", request)
        if request.__finished and reason.check(defer.CancelledError):
            pass
        elif request.__finished:
            # `request.processingFailed` calls `log.err`, we use it for consistency
            log.err(reason, "request %r failed" % request)
        elif hasattr(request, 'processingFailed'):
            # processingFailed call `finish`
            request.processingFailed(reason)
        else:
            # 'request' doesn't have internal twisted method 'processingFailed'
            self._processingFailedFallback(request, reason)

        if not request.__finished:
            request.finish()

    def _processingFailedFallback(self, request, reason):
        body = b"Processing Failed"
        self.setResponseCode(500)
        self.setHeader(b'content-type', b"text/plain")
        self.setHeader(b'content-length', intToBytes(len(body)))
        self.write(body)
        request.finish()

    def _ignoreCancelledError(self, e, request):
        e.trap(defer.CancelledError)
        logger.debug("request %r doesn't support cancelling", request)

    def requestFinished(self, reason, request, bodyd):
        request.__finished = True
        logger.debug("request finished due to %s", reason)
        if reason:
            bodyd.cancel()

    def render(self, request):

        bodyd = super(Resource, self).render(request)
        if not isinstance(bodyd, defer.Deferred):
            return bodyd

        request.__finished = False
        request.notifyFinish().addBoth(self.requestFinished, request, bodyd)

        bodyd.addCallbacks(
            callback=lambda body: self.finishRequest(request, body),
            errback=lambda reason: self.failRequest(request, reason),
        )

        return NOT_DONE_YET


class LeafResourceMixin(object):

    isLeaf = True

    def getChild(self, path, request):
        if path == "":
            return self
        else:
            return super(LeafResourceMixin, self).getChild(path, request)


@zope.interface.implementer(IResource)
class MergedResource(object):

    def __init__(self, resources):
        self.resources = resources

    @property
    def isLeaf(self):
        return all(res.isLeaf for res in self.resources)

    def isIgnoredResource(self, resource):
        return isinstance(resource, ErrorPage)

    def getChildWithDefault(self, name, request):
        all_subs = (
            res if res.isLeaf else res.getChildWithDefault(name, request)
            for res in self.resources
        )
        subs = itertools.ifilterfalse(self.isIgnoredResource, all_subs)
        return type(self)(lazycol(subs))

    def render(self, request):
        allowedMethods = set()
        for res in self.resources:
            try:
                return res.render(request)
            except UnsupportedMethod as e:
                allowedMethods.update(e.allowedMethods)
        raise UnsupportedMethod(list(allowedMethods))


@zope.interface.implementer(IResource)
class DynamicResource(object):

    isLeaf = True

    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args or ()
        self.kwargs = kwargs or {}

    def getChildWithDefault(self, name, request):
        return self

    def render(self, request):
        d = defer.maybeDeferred(self.fn, request, *self.args, **self.kwargs)
        d.addCallback(self._cbChild, request).addErrback(self._ebChild, request)
        return NOT_DONE_YET

    def _cbChild(self, child, request):
        request.render(getChildForRequest(child, request))

    def _ebChild(self, reason, request):
        request.processingFailed(reason)
        return reason


class WSGIResource(_WSGIResource):

    "Hacked WSGIResource. Allows serving wsgi apps through unix-socket."

    def render(self, request):
        # socket host has no 'port' attribute
        # but WSGI requires it - let it be `None`
        request.getHost().port = None
        return _WSGIResource.render(self, request)


# -- sites

class Site(_Site):

    "Silent version of twisted.web.server.Site. Don't write requests to twisted.log."

    def __init__(
            self, resource, logPath=None,
            displayTracebacks=None, **data
    ):
        assert logPath is None
        _Site.__init__(self, resource, logPath=None, **data)
        if displayTracebacks is None:
            self.displayTracebacks = settings.DEBUG
        else:
            self.displayTracebacks = displayTracebacks

    def _updateLogDateTime(self):
        # we don't need `self._logDateTime`
        pass

    def log(self, request):
        logger.debug(
            "%s %r - resp %s, sent %s",
            request.method,
            request.uri,
            request.code,
            request.sentLength or "-",
        )


class UnitTestSite(Site):

    """
    Special version of HTTPFactory. Tracks all active connections.
    WARN: Should be used only in unit-tests!
    """

    def __init__(self, *args, **kwargs):
        Site.__init__(self, *args, **kwargs)
        # simulate WeakSet
        self._protocols = weakref.WeakKeyDictionary()

    def buildProtocol(self, *args, **kwargs):
        p = _Site.buildProtocol(self, *args, **kwargs)
        self._protocols[p] = None
        return p

    @defer.inlineCallbacks
    def loseAllConnections(self):
        for p in list(self._protocols):
            try:
                yield p.transport.loseConnection()
            except Exception:
                logger.exception("fail to close connection")


# -- consumer & producer

class StringBodyProducer(FileBodyProducer):
    def __init__(self, bytes):
        FileBodyProducer.__init__(self, StringIO(bytes))


@zope.interface.implementer(IConsumer)
class StringConsumer(object):

    def __init__(self, callLater=None):
        self._io = StringIO()
        self._callLater = callLater or reactor.callLater
        self._producer = None
        self._streaming = True
        self._pullCall = None

    def registerProducer(self, producer, streaming):
        self._streaming = streaming
        self._producer = producer
        if not streaming:
            if self._pullCall:
                self._pullCall.cancel()
            self._pullCall = self.callLater(0, self._pull)

    def unregisterProducer(self):
        if self._pullCall:
            self._pullCall.cancel()
            self._pullCall = None
        self._producer = None

    def _pull(self):
        self._producer.resumeProducing()

    def write(self, data):
        self._io.write(data)
        if not self._streaming and data:
            self._callLater(0, self._pull)

    def value(self):
        return self._io.getvalue()


# --- routing


class _EmptyResource(Resource):
    pass


def _flatten_url_dict(acc, d, prefix):
    if IResource.providedBy(d) or isinstance(d, basestring):
        acc.append((prefix, d))
    else:
        for k, v in dict(d).items():
            if k is None:
                pp = prefix
            else:
                pp = "%s/%s" % (prefix, k)
            _flatten_url_dict(acc, v, pp)


def _put_subresource(root, path, res2put):

    assert path

    cr = root
    for name in path[:-1]:
        new_cr = cr.getStaticEntity(name)
        if new_cr is None:
            new_cr = _EmptyResource()
            cr.reallyPutEntity(name, new_cr)
        cr = new_cr

    cure = cr.getStaticEntity(path[-1])
    if cure is not None:
        raise ValueError("invalid routing - duplicated entity %r" % "/".join(path))
    else:
        cr.reallyPutEntity(path[-1], res2put)


def buildResourceTree(resources):

    if IResource.providedBy(resources):
        return resources

    logger.debug("build resorce tree...")
    if isinstance(resources, collections.Mapping):
        res_list = []
        _flatten_url_dict(res_list, resources, "")
    else:
        res_list = list(resources)
    res_list.sort()

    roots = [v for k, v in res_list if not k]
    assert len(roots) <= 1
    logger.debug("resource tree is %r", res_list)

    if roots:
        root, = roots
    else:
        root = _EmptyResource()
    res_list = sorted((k, v) for k, v in res_list if k)

    for fpath, res in res_list:
        logger.debug("path %r, resource %r", fpath, res)
        if fpath.startswith("/"):
            fpath = fpath[1:]
        else:
            raise AssertionError("expected leading '/'")
        _put_subresource(root, fpath.split("/"), res)

    return root


def buildSite(restree, prefix=None):

    if isinstance(restree, _Site):
        assert not prefix
        return restree

    root = buildResourceTree(restree)
    if prefix:
        for pp in reversed(prefix.split("/")):
            root = buildResourceTree({pp: root})

    return Site(root)
