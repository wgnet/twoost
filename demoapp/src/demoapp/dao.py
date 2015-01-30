# coding: utf-8

import datetime

from twisted.application.service import Service

from twoost.dbtools import DBUsingMixin

import logging
logger = logging.getLogger(__name__)


class DBDaoService(DBUsingMixin, Service):

    def __init__(self, dbs, memcache=None):
        DBUsingMixin.__init__(self, dbs)
        # TODO: add caching ;)
        self.memcache = memcache

    def startService(self):
        logger.debug("start dbdao")
        # do any required initialization here ...

    def delete_event_by_id(self, event_id):
        return self.db_operation("DELETE FROM events WHERE id = ?", event_id)

    def get_event_by_id(self, event_id):
        return self.db_query_one("SELECT * FROM events WHERE id = ?", event_id)

    def all_events_after_dt(self, dt):
        return self.db_query_all("SELECT * FROM events WHERE created > ?", dt)

    def insert_new_event(self, event):
        return self.db_operation(
            "INSERT INTO events (id, payload, created) VALUES (?, ?, ?)",
            event['id'], event['payload'], datetime.datetime.now())
