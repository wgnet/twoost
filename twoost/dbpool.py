# coding: utf-8

import os
import functools

from twisted.enterprise.adbapi import ConnectionPool
from twisted.application import service
from twisted.python import reflect

import logging
logger = logging.getLogger(__name__)


__all__ = ['DatabaseService', 'make_dbpool']


class TwoostConnectionPool(ConnectionPool):

    def __init__(self, *args, **kwargs):
        ConnectionPool.__init__(self, **kwargs)
        self.cp_init_conn = kwargs.pop('cp_init_conn')
        if isinstance(self.cp_init_conn, basestring):
            self.cp_init_conn = reflect.namedAny(self.cp_init_conn)

    def _disconnect_current(self):
        conn = self.connections.get(self.threadID())
        if conn:
            logger.debug("disconnect %r", conn)
            self.disconnect(conn)

    def retry_on_error(self, e):
        return False

    def _mk_retry(fn):

        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except self.retry_on_errors as e:
                if not self.retry_on_error(e):
                    raise
                # connection lost etc.
                logger.exception("retry operation %s(*%s, **%s)", fn.__name__, args, kwargs)
                self._disconnect_current()
                return fn(self, *args, **kwargs)

        return wrapper

    # not wrap runInteraction & runWithConnection
    runQuery = _mk_retry(ConnectionPool.runQuery)
    runOperation = _mk_retry(ConnectionPool.runOperation)

    def prepare_connection(self, connection):
        if self.cp_init_conn:
            logger.debug("init db connection - run %s on %s", self.cp_init_conn, connection)
            self.cp_init_conn(connection)

    def connect(self):
        new_connection = self.threadID() not in self.connections
        conn = ConnectionPool.connect(self)
        if new_connection:
            self.prepare_connection(conn)
        return conn


class PGSqlConnectionPool(TwoostConnectionPool):

    def __init__(self, *args, **kwargs):
        import psycopg2
        import psycopg2.extras
        ConnectionPool.__init__(self, *args, cursor_factory=psycopg2.extras.DictCursor, **kwargs)
        self.pg_extras = kwargs.pop('pg_extras', ['hstore', 'json'])
        self._retry_on_errors = (psycopg2.OperationalError, psycopg2.InterfaceError)

    def retry_on_error(self, e):
        return isinstance(e, self._retry_on_errors)

    def prepare_connection(self, connection):
        import psycopg2.extras
        for e in self.pg_extras:
            getattr(psycopg2.extras, "register_" + e)(connection)
        return TwoostConnectionPool.prepare_connection(connection)


class MySQLConnectionPool(TwoostConnectionPool):

    def __init__(self, *args, **kwargs):
        import MySQLdb
        import MySQLdb.cursors
        ConnectionPool.__init__(self, *args, cursorclass=MySQLdb.cursors.DictCursor, **kwargs)
        self.retry_on_errors = (MySQLdb.OperationalError,)

    def retry_on_error(self, e):
        return isinstance(e, self._retry_on_errors) and e[0] in (2006, 2013)


class _SQLiteConnectionPool(TwoostConnectionPool):

    def prepare_connection(self, connection):
        import sqlite3
        connection.row_factory = sqlite3.Row
        return TwoostConnectionPool.prepare_connection(self, connection)


# ---

def make_sqlite_dbpool(db):

    import sqlite3

    db = db.copy()
    db['database'] = os.path.expandvars(db['database'])

    logger.debug("connecting to sqlite db %r", db['database'])
    return _SQLiteConnectionPool(
        'sqlite3',
        detect_types=sqlite3.PARSE_DECLTYPES,
        check_same_thread=False,
        **db
    )


def make_mysql_dbpool(db):

    db = dict(db)
    db['db'] = db.pop('database')
    db['passwd'] = db.pop('password')
    db.setdefault('cp_reconnect', True)
    db.setdefault('charset', 'utf8')
    db.setdefault('host', 'localhost')
    db.setdefault('port', 3306)

    logger.debug("connecting to mysqldb %r", db['db'])
    return MySQLConnectionPool('MySQLdb', **db)


def make_pgsql_dbpool(db):

    db = dict(db)
    db.setdefault('cp_reconnect', True)
    db.setdefault('host', 'localhost')
    db.setdefault('port', 5432)

    logger.debug("connecting to pgsql %r", db['database'])
    return PGSqlConnectionPool('psycopg2', **db)


def make_dbpool(db):
    db = dict(db)
    driver = db.pop('driver')
    return {
        'mysql': make_mysql_dbpool,
        'pgsql': make_pgsql_dbpool,
        'sqlite': make_sqlite_dbpool,
    }[driver](db)


# --- integration with twisted app framework

class DatabaseService(service.Service):

    name = 'dbs'

    def __init__(self, databases):
        self.databases = dict(databases)
        self.db_pools = {}

    def startService(self):
        service.Service.startService(self)
        logger.debug("create dbpools...")
        for db_name, db in self.databases.items():
            logger.info("connect to db %r", db_name)
            dbpool = make_dbpool(db)
            self.db_pools[db_name] = dbpool
        logger.debug("all dbpools has been created")

    def stopService(self):
        logger.debug("destroy dbpools...")
        for db, dbpool in self.db_pools.items():
            logger.debug("close dbpool %r", db)
            dbpool.close()
        logger.debug("all dbpools has been destroyed")
        service.Service.stopService(self)

    def __getitem__(self, name):
        return self.db_pools[name]

    def __getstate__(self):
        state = service.Service.__getstate__(self)
        state['db_pools'] = None
        return state
