# coding: utf-8

from twisted.internet import reactor, defer
from twisted.application import service
from twisted.python import reflect
from twisted.web import client

from twoost import httprpc, authhmac
from twoost.timed import withTimeout

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'RPCProxyService',
    'make_rpc_proxy',
]


class LoopRPCProxy(service.Service):

    def __init__(self, target, timeout=60.0):
        self.target = target
        self.timeout = timeout
        self.callRemote = withTimeout(timeout)(self._call_remote)

    def _call_remote(self, method, *args):
        return getattr(self.target, method)(*args)


class _NoiselessHTTP11ClientFactory(client.HTTPConnectionPool._factory):
    noisy = False


class _HTTPClientProxyService(service.Service):

    def __init__(self, http_pool, proxy):
        self.http_pool = http_pool
        self.proxy = proxy

    def callRemote(self, method, *args):
        return self.proxy.callRemote(method, *args)

    @defer.inlineCallbacks
    def stopService(self):
        logger.debug("close http pool %r", self.http_pool)
        yield self.http_pool.closeCachedConnections()
        yield service.Service.stopService(self)


def make_http_pool_and_agent(params):

    cp_size = params.get('cp_size', 5)
    c_timeout = params.get('c_timeout', 30.0)

    # XXX: more extensibility
    auth = params.get('auth', 'authhmac')
    assert not auth or auth.lower() in ['none', 'authhmac', 'basic', 'digest']

    http_pool = client.HTTPConnectionPool(reactor)
    http_pool._factory = _NoiselessHTTP11ClientFactory
    http_pool.retryAutomatically = False
    http_pool.maxPersistentPerHost = cp_size
    agent = client.Agent(reactor, pool=http_pool, connectTimeout=c_timeout)

    if not auth or auth.lower() == 'none':
        pass
    elif auth.lower() == 'authhmac':
        access_key = params['access_key']
        secret_key = params.get('secret_key')
        agent = authhmac.AuthHMACAgent(agent, access_key, secret_key)
    elif auth.lower() == 'basic':
        username = params['username']
        password = params.get('password')
        agent = httprpc.BasicAuthAgent(agent, username, password)
    else:
        raise AssertionError("unknown %r auth" % auth)

    return http_pool, agent


def make_xmlrpc_proxy(params):
    url = params.get('url')
    timeout = params.get('timeout', 60.0)
    http_pool, agent = make_http_pool_and_agent(params)
    logger.debug("create xml-proxy, url %r", url)
    proxy = httprpc.XMLRPCProxy(url, agent=agent, timeout=timeout)
    return _HTTPClientProxyService(http_pool, proxy)


def make_dumbrpc_proxy(params):
    url = params.get('url')
    timeout = params.get('timeout', 60.0)
    http_pool, agent = make_http_pool_and_agent(params)
    logger.debug("create dumprpc-proxy, url %r", url)
    proxy = httprpc.DumbRPCProxy(url, agent=agent, timeout=timeout)
    return _HTTPClientProxyService(http_pool, proxy)


def make_loop_proxy(params):
    target = params.get('target')
    timeout = params.get('timeout', 60.0)
    if isinstance(target, basestring):
        target = reflect.namedAny(target)
    logger.debug("create loop-rpc-proxy, target is %r", target)
    return LoopRPCProxy(target=target, timeout=timeout)


RPC_PROXY_FACTORY = {
    'xmlrpc': make_xmlrpc_proxy,
    'dumbrpc': make_dumbrpc_proxy,
    'loop': make_loop_proxy,
}


def make_rpc_proxy(params):
    params = dict(params)
    protocol = params.pop('protocol')
    return RPC_PROXY_FACTORY[protocol](params)


# --- integration with twisted app framework

class RPCProxyService(service.MultiService):

    name = 'rpcps'

    def __init__(self, proxies):
        service.MultiService.__init__(self)
        self.proxies = dict(proxies)
        self.rpc_proxies = {}

    def startService(self):
        logger.debug("create rpc_proxies...")
        for client_name, params in self.proxies.items():
            logger.info("connect to %r, params %r", client_name, params)
            client = make_rpc_proxy(params)
            self.rpc_proxies[client_name] = client
            if service.IService.providedBy(client):
                self.addService(client)
        service.Service.startService(self)

    def makeCaller(self, connection, method):
        def call(*args):
            return self[connection].callRemote(method, *args)
        return call

    def __getitem__(self, proxy_name):
        return self.rpc_proxies[proxy_name]

    def __getstate__(self):
        state = service.Service.__getstate__(self)
        state['rpc_proxies'] = None
        return state
