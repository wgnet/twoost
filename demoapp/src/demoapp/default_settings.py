# coding: utf-8

import os

DEBUG = True

WORKERS_COUNT = {
    'demoapp-webapi': 1,
    'demoapp-dbworker': 1,
}

DATABASES = {
    'default': {
        'driver': 'sqlite',
        'database': os.path.expanduser("~/demoapp.db"),
    },
}

AMQP_CONNECTIONS = {
    'default': {
        'host': 'localhost',
        'port': 5672,
        'vhost': '/',
        'user': 'guest',
        'password': 'guest',
    },
}

AMQP_SCHEMAS = {
    'default': {
        'exchange': {
            'new_event': {'type': 'topic'},
        },
        'queue': {
            'incoming_events': {'durable': True},
        },
        'bind': [
            ('new_event', 'incoming_events'),
        ],
    },
}
