# coding: utf-8

from __future__ import absolute_import
import os

DEBUG = True
ADMINS = []

WEB_ENDPOINT = os.path.expandvars("unix:/tmp/www/$TWOOST_WORKERID.sock")

WORKERS_COUNT = {
    'demoapp-webapi': 2,
    'demoapp-dbworker': 1,
}

DATABASES = {
    'default': {
        'driver': 'sqlite',
        'database': os.path.expanduser("~/demoapp_dev.db"),
    },
}

AMQP_CONNECTIONS = {
    'default': {
        'user': "guest",
        'password': "guest",
        'vhost': "/",
    },
}

MEMCACHE_SERVERS = {
    'cache_x01': {'host': 'localhost'},
    'cache_x02': {'host': 'localhost'},
}
