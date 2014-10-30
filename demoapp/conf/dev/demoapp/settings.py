# coding: utf-8

DEBUG = True
ADMINS = []

WORKERS_COUNT = {
    'demoapp-webapi': 2,
    'demoapp-dbworker': 1,
}

DATABASES = {
    'demoapp': {
        'driver': 'sqlite',
        'database': ":memory:",
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
