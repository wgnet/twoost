# coding: utf-8

import itertools
import functools

from twisted.internet import defer
from twisted.protocols.memcache import MemCacheProtocol

from .pclient import (
    PersistentClientsCollectionService,
    PersistentClientFactory,
    PersistentClientProtocol,
)
from twoost._misc import merge_dicts


import logging
logger = logging.getLogger(__name__)


class _MemCacheProtocol(PersistentClientProtocol, MemCacheProtocol):

    def __init__(self, timeout=3, max_key_length=250):
        MemCacheProtocol.__init__(self, timeOut=timeout)
        self.MAX_KEY_LENGTH = max_key_length

    def connectionLost(self, reason):
        self.setTimeout(None)
        return MemCacheProtocol.connectionLost(self, reason)

    def connectionMade(self):
        self.protocolReady()


# ---

def _dlistIgnoreSomeErrors(ls):
    res = []
    for success, value in ls:
        if success:
            res.append(value)
    if res:
        return res
    else:
        # there is nothing but errors
        # return first error, other errors will be logged by Deferred.__del__
        raise defer.FirstError(ls[0][1], 0)


class _MemCacheMultiClientProxy(object):

    def __init__(self, memcaches, resolveClientNameByKey):
        self.memcaches = memcaches
        self.resolveClientNameByKey = resolveClientNameByKey

    def __buildProxyMethod(name):
        @functools.wraps(getattr(MemCacheProtocol, name))
        def method(self, key, *args, **kwargs):
            cname = self.resolveClientNameByKey(key)
            try:
                client = self.memcaches[cname]
            except KeyError:
                raise ValueError("unknown memcache server", cname)
            return getattr(client, name)(key, *args, **kwargs)
        return method

    for m in [
        'increment',
        'decrement',
        'replace',
        'add',
        'set',
        'checkAndSet',
        'append',
        'prepend',
        'get',
        'delete',
    ]:
        locals()[m] = __buildProxyMethod(m)

    def getMultiple(self, keys, withIdentifier=False, ignoreErrors=True):

        clients = sorted((self.resolveClientNameByKey(k), k) for k in keys)
        for cname, _ in clients:
            try:
                self.memcaches[cname]
            except LookupError:
                raise ValueError("unknown memcache server", cname)

        ds = [
            defer.maybeDeferred(
                self.memcaches[cn].getMultiple,
                [x[1] for x in ckeys],
                withIdentifier,
            )
            for cn, ckeys in itertools.groupby(clients, lambda x: x[0])
        ]
        dl = defer.DeferredList(
            ds,
            fireOnOneErrback=(not ignoreErrors),
            consumeErrors=False)

        if ignoreErrors:
            dl.addCallback(_dlistIgnoreSomeErrors)

        return dl.addCallback(merge_dicts)

    def version(self):
        raise NotImplementedError

    def stats(self):
        raise NotImplementedError

    def flushAll(self):
        raise NotImplementedError


class MemCacheFactory(PersistentClientFactory):
    protocol = MemCacheProtocol


class MemCacheService(PersistentClientsCollectionService):

    name = 'memcaches'
    factory = MemCacheFactory

    protocolProxiedMethods = [
        'increment',
        'decrement',
        'replace',
        'add',
        'set',
        'checkAndSet',
        'append',
        'prepend',
        'get',
        'getMultiple',
        'stats',
        'version',
        'delete',
        'flushAll',
    ]

    defaultParams = {
        'host': "localhost",
        'port': 11211,
        'reconnect_max_delay': 180,
        'reconnect_initial_delay': 0.1,
        'callretry_delay': 0,
    }

    def multiClient(self, resolveClientNameByKey):
        return _MemCacheMultiClientProxy(self, resolveClientNameByKey)
