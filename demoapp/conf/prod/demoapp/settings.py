from __future__ import absolute_import
import os

DEBUG = False
ADMINS = [
    ("Ivan", "ivan@example.com"),
    ("Petr", "petr@example.com"),
    ("Fedor", "fedor@example.com"),
]

WORKERS_COUNT = {
    'demoapp-webapi': 4,
    'demoapp-dbworker': 3,
}

DATABASES = {
    'default': {
        'driver': 'sqlite',
        'database': os.path.expanduser("~/demoapp.db"),
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

# see `logging.config.dictConfig` & `twoost.log.LoggingSettings`
# LOGGING = {...}
