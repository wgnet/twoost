# coding: utf8

import os
import socket
import getpass

from .conf import Config


class DefaultSettings(Config):

    DEBUG = False
    ADMINS = []

    WORKERS_COUNT = {}
    DATABASES = {}
    AMQP_CONNECTIONS = {}
    AMQP_SCHEMAS = {}
    RPC_PROXIES = {}
    MEMCACHE_SERVERS = {}

    WEB_ENDPOINT = os.path.expandvars("unix:$HOME/run/www/$TWOOST_WORKERID.sock")
    MANHOLE_SOCKET = os.path.expandvars("$HOME/run/manhole/$TWOOST_WORKERID.sock")

    EMAIL_DEFAULT_FROM = "{0}@{1}".format(getpass.getuser(), socket.gethostname())
    EMAIL_USER = None
    EMAIL_PASSWORD = None
    EMAIL_HOST = "localhost"
    EMAIL_PORT = 25

    LOGGING_CONFIG = 'logging.config.dictConfig'
    LOGGING = {'version': 1}

    PID_DIR = os.environ.get("TWOOST_PID_DIR") or os.path.expanduser("~/run")
