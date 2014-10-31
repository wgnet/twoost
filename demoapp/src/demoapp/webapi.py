# coding: utf8

import uuid

from twisted.application.service import Service
from twisted.internet import defer

import logging
logger = logging.getLogger(__name__)


# place here any public/private API's

class WebAPIService(Service):

    def __init__(self, queue_event_callback):
        self.queue_event_callback = queue_event_callback

    @defer.inlineCallbacks
    def new_event(self, payload):
        # TODO: validate & normalize payload...
        id = uuid.uuid4().get_hex()
        yield self.queue_event_callback({
            'payload': str(payload),
            'id': id,
        })
        defer.returnValue({'id': id})
