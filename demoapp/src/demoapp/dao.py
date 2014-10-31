# coding: utf-8

import datetime

from twisted.application.service import Service

import logging
logger = logging.getLogger(__name__)


def single_row(rows):
    if not rows:
        return None
    elif len(rows) == 1:
        return rows[0]
    else:
        raise RuntimeError("multiple rows")


class DBDaoService(Service):

    def __init__(self, dbs, memcache=None):
        self.dbs = dbs
        # TODO: add caching ;)
        self.memcache = memcache

    def startService(self):
        logger.debug("start dbdao")
        # do any required initialization here ...

    # --- helpers

    def _query(self, sql, args=None, db='default'):
        logger.debug("query %r, %r", sql, args)
        return self.dbs[db].runQuery(sql, args)

    def _operation(self, sql, args=None, db='default'):
        logger.debug("operation %r, %r", sql, args)
        return self.dbs[db].runOperation(sql, args)

    def _interaction(self, fn, *args, **kwargs):
        db = kwargs.pop('db', 'default')
        logger.debug("call fn(*%r, **%r)", fn, args, kwargs)
        return self.dbs[db].runInteraction(fn, *args, **kwargs)

    # --- dal

    def delete_event_by_id(self, event_id):
        return self._operation("""
            DELETE FROM events
            WHERE id = ?
        """, [
            event_id,
        ])

    def get_event_by_id(self, event_id):
        return self._query("""
            SELECT *
            FROM events
            WHERE id = ?
        """, [
            event_id,
        ]).addCallback(single_row)

    def all_events_after_dt(self, dt):
        return self._query("""
            SELECT *
            FROM events
            WHERE created > ?
        """, [
            dt,
        ])

    def insert_new_event(self, event):
        return self._operation("""
            INSERT INTO events (id, payload, created)
            VALUES (?, ?, ?)
        """, [
            event['id'],
            event['payload'],
            datetime.datetime.now(),
        ])
