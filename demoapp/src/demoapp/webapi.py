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

        # validate & normalize payload...

        # normalize, here we add unique id to event
        uid = uuid.uuid4().get_hex()
        assert 'uid' not in payload
        payload['uid'] = uid

        yield self.queue_event_callback(payload)
        defer.returnValue({'uid': uid})
