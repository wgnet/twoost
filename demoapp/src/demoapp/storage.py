# coding: utf-8

import collections
import datetime

from twisted.application.service import Service

import logging
logger = logging.getLogger(__name__)


# -- generate sql -- helpers

def single_row(rows):
    if not rows:
        return None
    elif len(rows) == 1:
        return rows[0]
    else:
        raise RuntimeError("multiple rows")


def merge_rows(old_row, new_row):
    d = collections.defaultdict(lambda _: None)
    d.update(old_row)
    d.update(new_row)
    d['updated'] = datetime.datetime.now()
    return d


# -- DAO

class DBDaoService(Service):

    def __init__(self, dbs, memcache=None):
        self.dbs = dbs
        self.memcache = memcache

    def startService(self):
        logger.debug("start dbdao")
        # do any required initialization here ...

    # --- helpers

    def _query(self, sql, args=None):
        logger.debug("query %r, %r", sql, args)
        return self.dbs['demoapp'].runQuery(sql, args)

    def _operation(self, sql, args=None):
        logger.debug("operation %r, %r", sql, args)
        return self.dbs['demoapp'].runOperation(sql, args)

    def _interaction(self, fn, *args, **kwargs):
        logger.debug("call fn(*%r, **%r)", fn, args, kwargs)
        return self.dbs['demoapp'].runInteraction(fn, *args, **kwargs)

    # --- DAL implementation

    def delete_event_by_id(self, event_id):
        return self._operation("""
            DELETE FROM events
            WHERE id = %s
        """, [
            event_id,
        ])

    def get_event_by_id(self, event_id):
        return self._interaction(self._txn_get_event_by_id)

    def _txn_get_event_by_id(self, txn, event_id):
        txn.execute("""
            SELECT *
            FROM events
            WHERE id = %s
        """, [
            event_id,
        ])
        return single_row(txn.fetchall())

    def all_events_after_dt(self, dt):
        return self._query("""
            SELECT *
            FROM events
            WHERE created > %s
        """, [
            dt,
        ])

    def insert_new_event(self, event):
        return self._interaction(self._txn_insert_new_user, event)

    def _txn_insert_new_user(self, txn, event):
        txn.execute("""
            INSERT INTO events (kind, description, payload, created)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, [
            event['kind'],
            event['description'],
            event['payload'],
            datetime.datetime.now(),
        ])
        return txn.fetchone()[0]

    def insert_or_update_event(self, event):
        return self._interaction(self._txn_insert_or_update_event)

    def _txn_insert_or_update_event(self, txn, event):

        if 'id' in event:
            old_event = self._txn_get_event_by_id(event['id'])
        else:
            old_event = None

        if old_event is None:
            return self._txn_insert_new_user(event)
        else:
            if old_event['kind'] != event['kind']:
                raise ValueError("can't change event kind")
            txn.execute("""
                UPDATE events
                SET
                    description = %(description)s,
                    payload = %(payload)s,
                    updated = %(updated)s
                WHERE id = %(id)s
            """, merge_rows(old_event, event))
            return event['id']
    # ---
