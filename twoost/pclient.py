# coding: utf-8

from __future__ import print_function, division

"""
Persisten client.
"""

import random
import collections
import functools

from twisted.internet import protocol, endpoints, defer
from twisted.internet.error import ConnectionClosed
from twisted.python import failure
from twisted.application import service

from twoost._misc import get_attached_clock, required_attr

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'PersistentClientFactory',
    'PersistentClientService',
    'PersistentClientProtocol',
    'NoAcviteConnection',
]


# -- client factory

class NoAcviteConnection(Exception):
    pass


class _SpiriousReconnectingClientFactory(protocol.ReconnectingClientFactory):

    disconnectDelay = None  # disabled by default

    maxDelay = 3600
    initialDelay = 1.0
    factor = 1.6180339887498948
    jitter = 0.11962656472

    # --
    _disconnectCallID = None

    def startedConnecting(self, connector):
        self.connector = connector
        protocol.ReconnectingClientFactory.startedConnecting(self, connector)

    def scheduledDisconnect(self):
        self._cancelDisconnectCall()
        if self.connector:
            logger.debug("scheduled disconnect of %s", self)
            self.connector.disconnect()

    def resetDelay(self):
        protocol.ReconnectingClientFactory.resetDelay(self)
        self._cancelDisconnectCall()
        if self.disconnectDelay:
            delay = random.normalvariate(self.disconnectDelay, self.jitter)
            clock = get_attached_clock(self)
            logger.debug("schedule to disconnect from %s in %s seconds", self, delay)
            self._disconnectCallID = clock.callLater(
                max(delay, 0),
                self.scheduledDisconnect,
            )

    def _cancelDisconnectCall(self):
        if self._disconnectCallID and self._disconnectCallID.active():
            self._disconnectCallID.cancel()
        self._disconnectCallID = None

    def clientConnectionFailed(self, *args, **kwargs):
        self._cancelDisconnectCall()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, *args, **kwargs)

    def clientConnectionLost(self, *args, **kwargs):
        self._cancelDisconnectCall()
        protocol.ReconnectingClientFactory.clientConnectionLost(self, *args, **kwargs)

    def __getstate__(self):
        s = protocol.ReconnectingClientFactory.__getstate__(self)
        s.pop('_disconnectCallID', None)
        return s

    def stopFactory(self):
        self._cancelDisconnectCall()
        return protocol.ReconnectingClientFactory.stopFactory(self)


class PersistentClientProtocol(object, protocol.BaseProtocol):

    def connectionMade(self):
        self.factory.clientReady(self)


class PersistentClientFactory(_SpiriousReconnectingClientFactory):

    # list of proxied methods (delegate to self.client)
    proxiedMethods = []

    # raised when there is no active connection
    noClientError = NoAcviteConnection

    # retry method calls on disconnect
    retryDelay = 5  # secs
    retryOnErrors = [ConnectionClosed]
    retryMaxCount = 250

    # autoreconnect
    initialDelay = 0.5
    maxDelay = 600  # 10 minutes
    maxRetries = None

    # autodicsonnect
    disconnectDelay = None

    # -- initial state --
    client = None

    _notify_disconnect = None
    _delayed_calls = None
    _delayed_calls_cnt = 0
    _incomplete_client = None

    def needToRetryCall(self, name, e):
        return e.check(*self.retryOnErrors)

    def callClient(self, name, *args, **kwargs):

        if self.client:
            wm = getattr(self.client, name)
            d = defer.maybeDeferred(wm, *args, **kwargs)
            if self.retryDelay:
                def handle_connection_done(e):
                    if self.needToRetryCall(name, e):
                        return self._callClientDelayed(name, *args, **kwargs)
                    else:
                        return e
                d.addErrback(handle_connection_done)
            return d
        else:
            return self._callClientDelayed(name, *args, **kwargs)

    def _buildProxyMethod(self, name):
        return functools.partial(self.callClient, name)

    def _callClientDelayed(self, method, *args, **kwargs):

        rd = self.retryDelay
        if not rd or not self.continueTrying or rd < self.delay:
            return failure.Failure(self.noClientError("no active client"))

        if len(self._delayed_calls) > self.retryMaxCount:
            return failure.Failure(
                self.noClientError("no active client - too many delayed calls"))

        logger.debug("schedule %s(*%r, **%r)", method, args, kwargs)

        rcall = None
        self._delayed_calls_cnt += 1
        x = self._delayed_calls_cnt

        def on_timeout():
            _, d = self._delayed_calls.pop(x, (None, None))
            if d and not d.called:
                d.errback(self.noClientError("no active client - timeout"))

        def rcall_cancel(x):
            self._delayed_calls.pop(x, None)
            if rcall and rcall.active():
                rcall.cancel()
            return x

        clock = get_attached_clock(self)
        d = defer.Deferred().addBoth(rcall_cancel)

        delay = self.retryDelay
        if self.jitter:
            delay = random.normalvariate(delay, delay * self.jitter)

        rcall = clock.callLater(delay, on_timeout)
        self._delayed_calls[x] = ((method, args, kwargs), d)

        return d

    def _runDelayedCalls(self):
        assert self.client
        smc = list(self._delayed_calls.values())
        self._delayed_calls.clear()
        for (name, args, kwargs), d in smc:
            logger.debug("call %s(*%r, **%r)", name, args, kwargs)
            wm = getattr(self.client, name)
            dd = defer.maybeDeferred(wm, *args, **kwargs)
            dd.chainDeferred(d)

    def _cancelAllDelayedCalls(self):
        for _, d in list(self._delayed_calls.itervalues()):
            d.cancel()

    def __getattr__(self, name):
        if name in self.proxiedMethods:
            m = self._buildProxyMethod(name)
            setattr(self, name, m)
            return m
        raise AttributeError(name)

    def startFactory(self):
        if not self._delayed_calls:
            self._delayed_calls = collections.OrderedDict()
        _SpiriousReconnectingClientFactory.startFactory(self)
        logger.debug("start %s", self)

    def stopFactory(self):
        logger.debug("stop %s", self)
        self._cancelAllDelayedCalls()
        _SpiriousReconnectingClientFactory.stopFactory(self)

    @defer.inlineCallbacks
    def disconnectAndWait(self, connector):
        """For test purposes only. Don't use this method!"""
        self.stopTrying()
        if self._incomplete_client:
            yield defer.maybeDeferred(self._incomplete_client.transport.loseConnection)
        if not self.client:
            return
        self._notify_disconnect = defer.Deferred()
        yield self._notify_disconnect

    def buildProtocol(self, addr):
        logger.debug("build protocol %s", self.protocol)
        p = self.protocol()
        p.factory = self
        assert isinstance(p, protocol.BaseProtocol)
        self._incomplete_client = p
        return p

    def clientConnectionLost(self, connector, reason):
        logger.debug("connection lost: %s", reason)
        self.client = None
        _SpiriousReconnectingClientFactory.clientConnectionLost(self, connector, reason)

        if self._notify_disconnect:
            self._notify_disconnect.callback(None)
            self._notify_disconnect = None

    def clientReady(self, client):

        """Client MUST call this method when all init-conn stuff wad done."""

        logger.debug("client %s now is ready", client)
        self.client = client
        self.resetDelay()
        self._runDelayedCalls()

    def __getstate__(self):
        s = _SpiriousReconnectingClientFactory.__getstate__(self)
        s.pop('client', None)
        s.pop('_notify_disconnect', None)
        s.pop('_delayed_calls', None)
        s.pop('_delayed_calls_cnt', None)
        return s


# --- integration with app-framework


class ClientFactoryWithEndpointService(service.Service):

    def __init__(self, factory, endpoint):
        self.factory = factory
        self.endpoint = endpoint
        self._connd = None

    def startService(self):
        service.Service.startService(self)
        self.endpoint.connect(self.factory)

    @defer.inlineCallbacks
    def stopService(self):
        try:
            self.factory.stopTrying()
        except Exception:
            logger.warning("unable to stopTrying", exc_info=1)

        c = self.factory._incomplete_client
        t = getattr(c, 'protocol', None)
        if c is not None and t is not None:
            logger.debug("lose connection on %r", t)
            yield defer.maybeDeferred(t.loseConnection)
        else:
            logger.debug("no active transport for %r", self.factory)

        yield service.Service.stopService(self)


class PersistentClientService(service.MultiService):

    defaultPort = None
    defaultHost = 'localhost'

    factory = required_attr

    def __init__(self, connections):
        service.MultiService.__init__(self)
        self.connections = dict(connections)
        for connection, cparams in self.connections.items():
            self._initClientService(connection, cparams)

    def buildClientEndpoint(self, params):
        if not params.get('endpoint'):
            ep = "tcp:host=%s:port=%s" % (
                params.get('host', self.defaultHost),
                params.get('port', self.defaultPort),
            )
        else:
            assert not params.get('host')
            assert not params.get('port')
            ep = params['endpoint']
        return endpoints.clientFromString(ep)

    def buildClientFactory(self, params):
        params = dict(params)
        params.pop('endpoint', None)
        params.pop('host', None)
        params.pop('port', None)
        return self.factory(**params)

    def _initClientService(self, connection, params):
        logger.debug("init client service %r, params %r", connection, params)
        factory = self.buildClientFactory(params)
        endpoint = self.buildClientEndpoint(params)
        s = ClientFactoryWithEndpointService(factory, endpoint)
        s.setName(connection)
        self.addService(s)

    def __getitem__(self, name):
        return self.getServiceNamed(name).factory
