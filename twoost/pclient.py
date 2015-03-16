# coding: utf-8

from __future__ import print_function, division

"""
Persisten protocol.
"""

import random
import collections
import functools

import zope.interface

from twisted.internet import reactor, protocol, endpoints, defer
from twisted.internet.error import ConnectionClosed
from twisted.python import failure
from twisted.application import service

from twoost import health
from twoost._misc import TheProxy

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'IPersistentClientProtocol',
    'PersistentClientProtocol',
    'PersistentClientFactory',
    'PersistentClientService',
    'NoPersisentClientConnection',
    'PersistentClientsCollectionService',
]


class NoPersisentClientConnection(ConnectionClosed):
    pass


class IPersistentClientProtocol(zope.interface.Interface):

    def notifyProtocolReady():
        pass


@zope.interface.implementer(IPersistentClientProtocol)
class PersistentClientProtocol(protocol.Protocol):

    _protocol_ready_notifiers = None

    def notifyProtocolReady(self):
        if self._protocol_ready_notifiers is None:
            self._protocol_ready_notifiers = list()
        d = defer.Deferred()
        self._protocol_ready_notifiers.append(d)
        return d

    def protocolFailed(self, reason=None):
        ds = self._protocol_ready_notifiers or ()
        self._protocol_ready_notifiers = None
        for d in ds:
            d.errback(reason)

    def protocolReady(self):
        ds = self._protocol_ready_notifiers or ()
        self._protocol_ready_notifiers = None
        for d in ds:
            d.callback(self)


@zope.interface.implementer(health.IHealthChecker)
class PersistentClientService(service.Service):

    # list of proxied methods (delegate to self.protocol)
    protocolProxiedMethods = []

    # raised when there is no active connection
    noClientError = NoPersisentClientConnection

    # various params - (configurable via __init__)

    # reconnect
    reconnect_initial_delay = 0.5
    reconnect_max_delay = 1200
    reconnect_delay_factor = 1.6180339887498948
    reconnect_delay_jitter = 0.11962656472
    reconnect_max_retries = None

    # autodisconnect params
    disconnect_delay = None  # disabled by default

    # retryConnection calls after reconnect
    callretry_delay = 15
    callretry_max_count = 500

    # -- state
    clock = reactor

    _delayedRetry = None
    _connectingDeferred = None
    _protocol = None
    _protocol_ready = False
    _protocolStoppingDeferred = None
    _disconnectCallID = None
    _delayedCalls = None
    _delayedCallsCnt = 0

    def __init__(self, endpoint, factory, **kwargs):

        self.endpoint = endpoint
        self.factory = factory

        self._delayedCalls = collections.OrderedDict()
        self.reconnect_delay = self.reconnect_initial_delay
        self._reconnect_retries = 0

        for p in [
                'reconnect_initial_delay',
                'reconnect_max_delay',
                'reconnect_delay_factor',
                'reconnect_delay_jitter',
                'reconnect_max_retries',
                'disconnect_delay',
                'callretry_delay',
                'callretry_delay',
        ]:
            if p in kwargs:
                setattr(self, p, kwargs[p])

    def startService(self):
        logger.debug("start pclient service %r", self)
        service.Service.startService(self)
        self.retryConnection(reconnect_delay=0.0)

    @defer.inlineCallbacks
    def stopService(self):

        logger.debug("stop pclient service %r", self)
        yield defer.maybeDeferred(service.Service.stopService, self)

        if self._delayedRetry is not None and self._delayedRetry.active():
            logger.debug("cancel delayedRetry on %r", self)
            self._delayedRetry.cancel()
            self._delayedRetry = None

        if self._connectingDeferred is not None:
            self._connectingDeferred.cancel()
            try:
                yield self._connectingDeferred
            except defer.CancelledError:
                pass
            except Exception:
                logger.exception("cancel connecting of %r", self)
            finally:
                self._connectingDeferred = None

        if self._protocol is not None:
            self._protocolStoppingDeferred = defer.Deferred()
            self._protocol.transport.loseConnection()
            yield self._protocolStoppingDeferred

    # --- delayed calls

    def needToRetryProtocolCall(self, f):
        return f.check(ConnectionClosed)

    def protocolCall(self, method_name, *args, **kwargs):
        logger.debug("on %s call %s(%r, %r)", self, method_name, args, kwargs)

        p = self.getProtocol()
        if p:
            wm = getattr(self._protocol, method_name)
            logger.debug("on %s call %s(%r, %r)", wm, method_name, args, kwargs)
            d = defer.maybeDeferred(wm, *args, **kwargs)
            if self.callretry_delay:
                def handle_connection_done(e):
                    if self.needToRetryProtocolCall(e):
                        return self._protocolCallDelayed(method_name, *args, **kwargs)
                    else:
                        return e

                d.addErrback(handle_connection_done)
            return d
        else:
            return self._protocolCallDelayed(method_name, *args, **kwargs)

    def _buildProtocolCallProxy(self, name):
        return functools.partial(self.protocolCall, name)

    def __getattr__(self, name):
        if name in self.protocolProxiedMethods:
            m = self._buildProtocolCallProxy(name)
            setattr(self, name, m)
            return m
        raise AttributeError(name)

    def _protocolCallDelayed(self, method, *args, **kwargs):

        rd = self.callretry_delay
        if not rd or rd < self.reconnect_delay:
            return failure.Failure(self.noClientError("no active client"))

        if len(self._delayedCalls) > self.callretry_max_count:
            return failure.Failure(
                self.noClientError("no active client - too many delayed calls"))

        delay = self.callretry_delay
        if self.reconnect_delay_jitter:
            delay = random.normalvariate(
                delay,
                delay * self.reconnect_delay_jitter)

        def on_timeout():
            _, d = self._delayedCalls.pop(cid, (None, None))
            if d and not d.called:
                d.errback(self.noClientError("no active client - timeout"))

        def on_cancel(result):
            self._delayedCalls.pop(cid, None)
            if rcall and rcall.active():
                rcall.cancel()
            return result

        logger.debug("schedule %s(*%r, **%r)", method, args, kwargs)
        self._delayedCallsCnt += 1
        cid = self._delayedCallsCnt

        rcall = self.clock.callLater(delay, on_timeout)
        d = defer.Deferred().addBoth(on_cancel)
        self._delayedCalls[cid] = ((method, args, kwargs), d)

        return d

    def _runDelayedCalls(self):
        logger.debug("run delayed calls on %s", self)
        assert self.getProtocol()
        smc = list(self._delayedCalls.values())
        self._delayedCalls.clear()
        for (name, args, kwargs), d in smc:
            logger.debug("call %s(*%r, **%r)", name, args, kwargs)
            wm = getattr(self._protocol, name)
            dd = defer.maybeDeferred(wm, *args, **kwargs)
            dd.chainDeferred(d)

    def _dropDelayedCalls(self):
        logger.debug("drop delayed calls on %s", self)
        for _, d in list(self._delayedCalls.itervalues()):
            d.cancel()
        self._delayedCalls.clear()

    # --- connection stuff

    def _clientProtocolProxyConnected(self, protocol_proxy):
        assert isinstance(protocol_proxy, _PClientProtocolProxy)
        return self.clientConnected(protocol_proxy._protocol)

    def clientConnected(self, protocol):
        self._protocol = protocol

        if IPersistentClientProtocol.providedBy(protocol):
            prd = protocol.notifyProtocolReady()
            prd.addCallback(
                lambda _: self.clientProtocolReady(protocol)
            ).addErrback(
                self.clientProtocolFailed
            )
        else:
            self.clientProtocolReady(protocol)

    def clientConnectionFailed(self, reason):
        logger.debug("connection on %s failed: %s", self, reason)
        self._cancelDisconnectCall()
        self.retryConnection()

    def clientConnectionLost(self, reason):
        logger.debug("connection on %s lost: %s", self, reason)
        self._protocol = None
        self._cancelDisconnectCall()
        if self._protocolStoppingDeferred is not None:
            d = self._protocolStoppingDeferred
            self._protocolStoppingDeferred = None
            d.callback(None)
        self.retryConnection()

    def clientProtocolFailed(self, reason):
        self._protocol_ready = False
        logger.error("%s protocol failed: %s", self, reason)

    def getProtocol(self):
        if self._protocol and self._protocol_ready:
            return self._protocol

    def clientProtocolReady(self, protocol):
        logger.debug("client protocol ready")
        self._protocol_ready = True
        self.resetReconnectDelay()
        self._runDelayedCalls()
        self._scheduleDisconnect()

    def _clearConnectionAttempt(self, result):
        self._connectingDeferred = None
        return result

    def _cancelDisconnectCall(self):
        if self._disconnectCallID and self._disconnectCallID.active():
            self._disconnectCallID.cancel()
        self._disconnectCallID = None

    def _scheduleDisconnect(self):
        self._cancelDisconnectCall()
        if self.disconnect_delay:
            reconnect_delay = random.normalvariate(
                self.disconnect_delay, self.reconnect_delay_jitter)
            logger.debug("schedule to disconnect from %s in %s seconds", self, reconnect_delay)
            self._disconnectCallID = self.clock.callLater(
                max(reconnect_delay, 0),
                self.dropConnection)

    @defer.inlineCallbacks
    def dropConnection(self):
        self._cancelDisconnectCall()
        if self._protocol is not None:
            logger.debug("lose connection on %r", self._protocol)
            self._protocolStoppingDeferred = defer.Deferred()
            self._protocol.transport.loseConnection()
            yield self._protocolStoppingDeferred

    def retryConnection(self, reconnect_delay=None):
        """ Have this connector connect again, after a suitable reconnect_delay."""

        if not self.running:
            logger.debug("Service stopped, skip connection on %s", self.endpoint)
            return

        if self.reconnect_max_retries and (self._reconnect_retries >= self.reconnect_max_retries):
            logger.info(
                "Abandoning %s after %d _reconnect_retries.",
                self.endpoint, self._reconnect_retries)
            return

        if self._connectingDeferred is not None:
            logger.debug("Abandoning connection for %s because another attempt.", self.endpoint)
            return

        if reconnect_delay is None:
            self.reconnect_delay = min(
                self.reconnect_delay * self.reconnect_delay_factor,
                self.reconnect_max_delay)
            if self.reconnect_delay_jitter:
                self.reconnect_delay = random.normalvariate(
                    self.reconnect_delay,
                    self.reconnect_delay * self.reconnect_delay_jitter)
            reconnect_delay = self.reconnect_delay

        logger.debug("Will retry connection %s in %s seconds.", self.endpoint, reconnect_delay)

        def reconnector():
            logger.debug("do reconnect of %r", self)
            self._reconnect_retries += 1
            self._connectingDeferred = self.endpoint.connect(
                _PClientProtocolFactoryProxy(self.factory, self)
            ). addBoth(
                self._clearConnectionAttempt
            ).addCallback(
                self._clientProtocolProxyConnected
            ).addErrback(
                self.clientConnectionFailed
            )

        self._delayedRetry = self.clock.callLater(reconnect_delay, reconnector)

    def resetReconnectDelay(self):
        """
        Call this method after a successful connection: it resets the reconnect_delay and
        the retryConnection counter.
        """
        self.reconnect_delay = min(self.reconnect_initial_delay, self.reconnect_max_delay)
        self._reconnect_retries = 0
        self._scheduleDisconnect()

    def checkHealth(self):
        if self.getProtocol():
            return True, ""
        else:
            return False, "reconnect in %s secs" % int(self.reconnect_delay)


class _PClientProtocolProxy(TheProxy):
    """A proxy for a Protocol to provide connectionLost notification."""

    def __init__(self, protocol, clientService):
        TheProxy.__init__(self, protocol)
        self._protocol = protocol
        self._clientService = clientService

    def connectionLost(self, reason):
        result = self._protocol.connectionLost(reason)
        self._clientService.clientConnectionLost(reason)
        return result


class _PClientProtocolFactoryProxy(TheProxy):
    """A wrapper for a ProtocolFactory to facilitate restarting Protocols."""

    _protocolProxyFactory = _PClientProtocolProxy

    def __init__(self, protocolFactory, clientService):
        TheProxy.__init__(self, protocolFactory)
        self.protocolFactory = protocolFactory
        self.clientService = clientService

    def buildProtocol(self, addr):
        protocol = self.protocolFactory.buildProtocol(addr)
        wrappedProtocol = self._protocolProxyFactory(protocol, self.clientService)
        return wrappedProtocol


class PersistentClientFactory(protocol.Factory):
    # WARN: just Factory, not ClientFactory!

    def __init__(self, *args, **kwargs):
        pass


class PersistentClientsCollectionService(service.MultiService):

    factory = None
    clientService = PersistentClientService
    protocolProxiedMethods = None
    defaultParams = {}

    def __init__(self, connections):
        service.MultiService.__init__(self)
        self.connections = dict(connections)
        for connection, cparams in self.connections.items():
            self._initClientService(connection, cparams)

    def buildClientEndpoint(self, params):
        if not params.get('endpoint'):
            ep = "tcp:host=%s:port=%s" % (params['host'], params['port'])
        else:
            assert not params.get('host')
            assert not params.get('port')
            ep = params['endpoint']
        return endpoints.clientFromString(reactor, ep)

    def buildClientFactory(self, params):
        return self.factory(**params)

    def buildClientService(self, endpoint, factory, params):
        s = self.clientService(endpoint, factory, **params)
        if self.protocolProxiedMethods:
            s.protocolProxiedMethods = self.protocolProxiedMethods
        return s

    def _initClientService(self, connection, params):

        p = dict(self.defaultParams)
        p.update(params)
        logger.debug("create protocol service %r, params %r", connection, p)

        factory = self.buildClientFactory(p)
        endpoint = self.buildClientEndpoint(p)
        service = self.buildClientService(endpoint, factory, p)

        service.setName(connection)
        service.setServiceParent(self)

    def __getitem__(self, name):
        return self.getServiceNamed(name)
