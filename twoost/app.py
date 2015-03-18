# coding: utf-8

from __future__ import print_function, division, absolute_import

import os

from twisted.internet import endpoints, defer, reactor, task
from twisted.application import internet, service

from twoost import log, geninit, _misc
from twoost.conf import settings
from twoost._misc import subdict, mkdir_p

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'attach_service',
    'react_app',
    'build_timer',
    'build_dbs',
    'build_web',
    'build_amqps',
    'build_rpcps',
    'build_memcache',
    'build_manhole',
    'build_health',
    'build_server',
    'build_client',
    'AppWorker',
]


# -- common

def attach_service(app, s):
    logger.info("attach service %s to application", s)
    s.setServiceParent(app)
    return s


def react_app(app, main, argv=()):

    @defer.inlineCallbacks
    def appless_main(reactor):
        yield defer.maybeDeferred(service.IService(app).startService)
        try:
            yield defer.maybeDeferred(main, *argv)
        finally:
            yield defer.maybeDeferred(service.IService(app).stopService)

    task.react(appless_main)


def build_timer(app, when, callback):

    if isinstance(when, (list, tuple)):
        assert len(when) == 5
        when = " ".join(when)

    try:
        interval = float(when)
    except (TypeError, ValueError):
        interval = None

    from twoost import cron
    logger.debug("build timer %r, callback %r", when, callback)
    if interval is not None:
        return attach_service(app, cron.IntervalTimerService(interval, callback))
    else:
        return attach_service(app, cron.CrontabTimerService(when, callback))


# -- generic server & client

def build_server(app, factory, endpoint):
    logger.debug("serve %s on %s", factory, endpoint)
    ept = endpoints.serverFromString(reactor, endpoint)
    ss = internet.StreamServerEndpointService(ept, factory)
    return attach_service(app, ss)


def build_client(app, client_factory, endpoint, params=None):
    from twoost import pclient
    logger.debug("connect %s to %s", client_factory, endpoint)
    ept = endpoints.clientFromString(reactor, endpoint)
    ss = pclient.PersistentClientService(ept, client_factory, **(params or {}))
    return attach_service(app, ss)


# -- twoost components

def build_dbs(app, active_databases=None):
    from twoost import dbpool
    logger.debug("build dbpool service")
    dbs = dbpool.DatabaseService(subdict(settings.DATABASES, active_databases))
    return attach_service(app, dbs)


def build_amqps(app, active_connections=None):

    from twoost import amqp
    connections = settings.AMQP_CONNECTIONS
    schemas = settings.AMQP_SCHEMAS
    logger.debug("build amqps service, connections %s", active_connections)

    d = subdict(connections, active_connections)
    for conn, params in d.items():
        d[conn] = dict(params)
        if conn in schemas:
            d[conn]['schema'] = schemas[conn]

    return attach_service(app, amqp.AMQPCollectionService(d))


def build_web(app, site, prefix=None, endpoint=None):
    from twoost import web
    logger.debug("build web service")
    endpoint = endpoint or settings.WEB_ENDPOINT
    if endpoint.startswith("unix:"):
        filename = endpoint[5:]
        mkdir_p(os.path.dirname(filename))
    site = web.buildSite(site, prefix)
    return build_server(app, site, endpoint)


def build_rpcps(app, active_proxies=None):
    from twoost import rpcproxy
    proxies = subdict(settings.RPC_PROXIES, active_proxies)
    logger.debug("build rpc proxies")
    return attach_service(app, rpcproxy.RPCProxyService(proxies))


def build_manhole(app, namespace=None):

    if not settings.DEBUG:
        logger.debug("don't create manhole server - production mode")
        return

    import twisted
    from twoost.manhole import AnonymousShellFactory
    from twisted.application.internet import UNIXServer

    workerid = app.workerid
    socket_file = os.path.join(settings.MANHOLE_SOCKET_DIR, workerid)
    mkdir_p(os.path.dirname(socket_file))

    namespace = dict(namespace or {})
    if not namespace:
        namespace.update({
            'twisted': twisted,
            'app': app,
            'settings': settings,
        })
    f = AnonymousShellFactory(namespace)

    logger.info("serve shell on %r socket", socket_file)

    # only '0600' mode allowed here!
    ss = UNIXServer(address=socket_file, factory=f, mode=0600, wantPID=1)

    return attach_service(app, ss)


def build_health(app):

    from twoost.health import HealthCheckFactory
    from twisted.application.internet import UNIXServer

    mode = settings.HEALTHCHECK_SOCKET_MODE
    workerid = app.workerid
    socket_file = os.path.join(settings.HEALTHCHECK_SOCKET_DIR, workerid)
    mkdir_p(os.path.dirname(socket_file))

    fct = HealthCheckFactory(app)

    logger.debug("serve health checker on %r socket", socket_file)
    ss = UNIXServer(address=socket_file, factory=fct, mode=mode, wantPID=1)

    return attach_service(app, ss)


def build_memcache(app, active_servers=None):
    from twoost import memcache
    servers = settings.MEMCACHE_SERVERS
    logger.debug("build memcache service, connections %s", servers)
    return attach_service(app, memcache.MemCacheService(subdict(servers, active_servers)))


# --- integration with 'geninit'

class AppWorker(geninit.Worker):

    healthcheck_timeout = 20

    @property
    def log_dir(self):
        return settings.LOG_DIR

    @property
    def pid_dir(self):
        return settings.PID_DIR

    @property
    def workers(self):
        return settings.WORKERS_COUNT.get(self.appname, 1)

    def init_logging(self, workerid):
        log.setup_logging(self.appname)

    def main(self, args=None):
        self.init_settings()
        return geninit.Worker.main(self, args)

    def create_app(self, workerid):
        app = service.Application(workerid)
        app.workerid = workerid
        self.crte_app(app, workerid)
        self.init_app(app, workerid)
        return app

    def crte_app(self, app, workerid):
        self.init_settings()
        self.init_logging(workerid)
        build_health(app)

    def read_worker_health(self, workerid, timeout=10):
        from twoost.health import parseServicesHealth
        self.init_settings()
        sp = os.path.join(settings.HEALTHCHECK_SOCKET_DIR, workerid)
        body = _misc.slurp_unix_socket(sp, timeout=timeout)
        return parseServicesHealth(body)

    def init_settings(self):
        raise NotImplementedError

    def init_app(self, app, workerid):
        raise NotImplementedError
