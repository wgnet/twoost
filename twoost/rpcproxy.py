# coding: utf-8

from twisted.internet import reactor
from twisted.application import service
from twisted.python import reflect
from twisted.web import client

from twoost import httprpc
from twoost.timed import withTimeout

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'RPCProxyService',
    'make_rpc_proxy',
]


# ---

class LoopRPCProxy(object):

    def __init__(self, target, timeout=60.0):
        self.target = target
        self.timeout = timeout
        self.callRemote = withTimeout(timeout)(self._call_remote)

    def _call_remote(self, method, *args):
        return getattr(self.target, method)(*args)


class _NoiselessHTTP11ClientFactory(client.HTTPConnectionPool._factory):
    noisy = False


# ---

def make_xmlrpc_proxy(params):

    p = dict(params)
    url = p.pop('url')
    timeout = p.pop('timeout', 60.0)

    cp_size = p.pop('cp_size', 5)
    c_timeout = p.pop('c_timeout', 30.0)
    assert not p

    http_pool = client.HTTPConnectionPool(reactor)
    http_pool._factory = _NoiselessHTTP11ClientFactory
    http_pool.retryAutomatically = False
    http_pool.maxPersistentPerHost = cp_size
    agent = client.Agent(reactor, pool=http_pool, connectTimeout=c_timeout)

    logger.debug("create xml-proxy, url %r", url)
    return httprpc.XMLRPCProxy(url, agent=agent, timeout=timeout)


def make_dumbrpc_proxy(params):

    p = dict(params)
    url = p.pop('url')
    timeout = p.pop('timeout', 60.0)

    cp_size = p.pop('cp_size', 5)
    c_timeout = p.pop('c_timeout', 30.0)
    assert not p

    http_pool = client.HTTPConnectionPool(reactor)
    http_pool._factory = _NoiselessHTTP11ClientFactory
    http_pool.retryAutomatically = False
    http_pool.maxPersistentPerHost = cp_size
    agent = client.Agent(reactor, pool=http_pool, connectTimeout=c_timeout)

    logger.debug("create dumprpc-proxy, url %r", url)
    return httprpc.DumbRPCProxy(url, agent=agent, timeout=timeout)


def make_loop_proxy(params):
    p = dict(params)
    target = p.pop('target')
    timeout = p.pop('timeout', 60.0)
    assert not p
    if isinstance(target, basestring):
        target = reflect.namedAny(target)
    logger.debug("create loop-rpc-proxy, target is %r", target)
    return LoopRPCProxy(target=target, timeout=timeout)


def make_rpc_proxy(params):
    params = dict(params)
    protocol = params.pop('protocol')
    return {
        'xmlrpc': make_xmlrpc_proxy,
        'httprpc': make_dumbrpc_proxy,
        'loop': make_loop_proxy,
    }[protocol](params)


# --- integration with twisted app framework

class RPCProxyService(service.MultiService):

    name = 'rpc-proxy-service'

    def __init__(self, proxies):
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

    def makeCaller(self, method, proxy_name='default'):
        def call(*args):
            return self.getProxy(proxy_name).callLater(method, *args)
        return call

    def __getitem__(self, proxy_name):
        return self.rpc_proxies[proxy_name]

    def __getstate__(self):
        state = service.Service.__getstate__(self)
        state['rpc_proxies'] = None
        return state
