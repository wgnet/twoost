# coding: utf8

from twisted.application import service

import logging
logger = logging.getLogger(__name__)


class StorageService(service.Service):

    def __init__(self, dao):
        self.dao = dao

    def process_event(self, msg):
        logger.info("new msg: %r", msg)
        return self.dao.insert_new_event(msg)
