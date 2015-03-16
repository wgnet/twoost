# coding: utf8

import os
import socket
import getpass

from .conf import Config, prop_lazy


class DefaultSettings(Config):

    DEBUG = False
    ADMINS = []

    WORKERS_COUNT = {}
    DATABASES = {}
    AMQP_CONNECTIONS = {}
    AMQP_SCHEMAS = {}
    RPC_PROXIES = {}
    MEMCACHE_SERVERS = {}

    WEB_ENDPOINT = prop_lazy(
        lambda: os.path.expandvars("unix:$HOME/run/www/$TWOOST_WORKERID.sock"))
    MANHOLE_SOCKET_DIR = prop_lazy(
        lambda: os.path.expandvars("$HOME/run/manhole"))
    HEALTHCHECK_SOCKET_DIR = prop_lazy(
        lambda: os.path.expandvars("$HOME/run/health"))
    HEALTHCHECK_SOCKET_MODE = 0700

    EMAIL_DEFAULT_FROM = prop_lazy(
        lambda: "{0}@{1}".format(getpass.getuser(), socket.gethostname()))

    EMAIL_USER = None
    EMAIL_PASSWORD = None
    EMAIL_HOST = "localhost"
    EMAIL_PORT = 25

    LOGGING_CONFIG_INIT = 'logging.config.dictConfig'
    LOGGING = {'version': 1}
    SENTRY_DSN = None

    PID_DIR = prop_lazy(
        lambda: os.environ.get("TWOOST_PID_DIR") or os.path.expanduser("~/run"))
