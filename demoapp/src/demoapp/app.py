# coding: utf-8

from twoost.app import *  # noqa
from twoost.conf import settings  # noqa

from . import __version__


class WebAPI(AppWorker):

    appname = 'demoapp-webapi'

    def init_app(self, app, workerid):

        # do your import HERE
        from twoost.httprpc import DumbRPCResource, XMLRPCResource
        from demoapp.webres import SlowHelloWorldResource
        from demoapp.webapi import WebAPIService

        # our webapi-worker hasn't direct access to DB
        # it only validates data & send it to our amqp
        amqps = build_amqps(app)

        webapi_service = attach_service(app, WebAPIService(
            queue_event_callback=amqps.makeSender(
                # routing_key_fn=lambda msg: msg['some_field'],
                connection='default',
                exchange='new_event',
            ),
            # we can use various callbacks here
            # `amqps.makeSender(...)` - send message via AMQP
            # `partial(proxy.remoteCall, 'method-name')` - execute RPC
            # `other_service.method_name` - direct access...
            # `partial(logger.info, "msg: %r")` -- just log
            # `lambda _: None` -- skip at all...
        ))

        # RPC fn = any function, which CAN return Deferred
        rpc_methods = {
            'version': lambda: __version__,
            'echo': lambda x: x,
            'new_event': webapi_service.new_event,
        }

        build_web(app, {
            'hello-world': SlowHelloWorldResource(),
            'rpc': {
                'dumbrpc': DumbRPCResource(rpc_methods),
                'xmlrpc': XMLRPCResource(rpc_methods),
                # TODO: add JSON-RPC ?
            }
        })

        # autodisabled unless settings.DEBUG switched on
        build_manhole(app, locals())


class DBWorker(AppWorker):

    appname = 'demoapp-dbworker'

    def init_app(self, app, workerid):
        from demoapp import storage, dbworker

        dbs = build_dbs(app)
        memcache = build_memcache(app)
        amqps = build_amqps(app)

        dao = attach_service(app, storage.DBDaoService(
            dbs=dbs,
            memcache=memcache,
        ))
        dbw_service = attach_service(app, dbworker.DBWorkerService(
            dao=dao,
            # links to other services, callbacks etc
        ))

        amqps.setupQueueConsuming(
            connection='default',
            queue='incoming_events',
            callback=dbw_service.process_event,
            parallel=5,  # how many messages we can process at the same time
        )

        build_manhole(app, locals())
