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

    reconnect = True
    init_conn = None

    def __init__(self, *args, **kwargs):
        ConnectionPool.__init__(self, *args, **kwargs)
        self.cp_init_conn = kwargs.pop('cp_init_conn', None)
        self._database = kwargs.get('database') or kwargs.get('db')
        if isinstance(self.cp_init_conn, basestring):
            self.cp_init_conn = reflect.namedAny(self.cp_init_conn)

    def _disconnect_current(self):
        conn = self.connections.get(self.threadID())
        if conn:
            logger.debug("disconnect %r from %s", conn, self._database)
            self.disconnect(conn)

    def retry_on_error(self, e):
        return False

    def _mk_log(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            logger.debug("db %s - %s(*%r, **%r)", self._database, fn.__name__, args, kwargs)
            return fn(self, *args, **kwargs)
        return wrapper

    def _mk_retry(fn):

        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)
            except Exception as e:
                if not self.retry_on_error(e):
                    raise
                # connection lost etc.
                logger.exception("retry operation %s(*%s, **%s)", fn.__name__, args, kwargs)
                self._disconnect_current()
                return fn(self, *args, **kwargs)

        return wrapper

    runQuery = _mk_log(_mk_retry(ConnectionPool.runQuery))
    runOperation = _mk_log(_mk_retry(ConnectionPool.runOperation))
    runInteraction = _mk_log(ConnectionPool.runInteraction)
    runWithConnection = _mk_log(ConnectionPool.runWithConnection)

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

        self.init_hstore = kwargs.pop('init_hstore', None)

        import psycopg2
        import psycopg2.extras
        TwoostConnectionPool.__init__(
            self, *args, cursor_factory=psycopg2.extras.DictCursor, **kwargs)

        self._retry_on_errors = (psycopg2.OperationalError, psycopg2.InterfaceError)

    def retry_on_error(self, e):
        return isinstance(e, self._retry_on_errors)

    def prepare_connection(self, connection):
        import psycopg2.extras

        if self.init_hstore:
            psycopg2.extras.register_hstore(connection)

        elif self.init_hstore is None:
            try:
                psycopg2.extras.register_hstore(connection)
            except psycopg2.ProgrammingError as e:
                if "hstore.sql" not in str(e):
                    raise
                logger.debug("hstore type not found in database")

        return TwoostConnectionPool.prepare_connection(self, connection)


class MySQLConnectionPool(TwoostConnectionPool):

    def __init__(self, *args, **kwargs):

        import MySQLdb
        import MySQLdb.cursors
        TwoostConnectionPool.__init__(
            self, *args, cursorclass=MySQLdb.cursors.DictCursor, **kwargs)

        self._retry_on_errors = (MySQLdb.OperationalError,)

    def retry_on_error(self, e):
        return isinstance(e, self._retry_on_errors) and e[0] in (2006, 2013)


class SQLiteConnectionPool(TwoostConnectionPool):

    def prepare_connection(self, connection):
        import sqlite3
        connection.row_factory = sqlite3.Row
        return TwoostConnectionPool.prepare_connection(self, connection)


# ---

def normalize_sqlite_db_conf(db):
    db['database'] = os.path.expandvars(db['database'])


def normalize_mysql_db_conf(db):
    if 'db' not in db and 'database' in db:
        db['db'] = db.pop('database')
    if 'passwd' not in db and 'password' in db:
        db['passwd'] = db.pop('password')


def normalize_pgsql_db_conf(db):
    db.setdefault('cp_reconnect', True)
    db.setdefault('host', 'localhost')
    db.setdefault('port', 5432)


def make_sqlite_dbpool(db):
    import sqlite3
    db = dict(db)
    normalize_sqlite_db_conf(db)
    logger.debug("connecting to sqlite db %r", db['database'])
    return SQLiteConnectionPool(
        'sqlite3',
        detect_types=sqlite3.PARSE_DECLTYPES,
        check_same_thread=False,
        **db
    )


def make_mysql_dbpool(db):
    db = dict(db)
    normalize_mysql_db_conf(db)
    logger.debug("connecting to mysqldb %r", db.get('db'))
    return MySQLConnectionPool('MySQLdb', **db)


def make_pgsql_dbpool(db):
    db = dict(db)
    normalize_pgsql_db_conf(db)
    logger.debug("connecting to pgsql %r", db.get('database'))
    return PGSqlConnectionPool('psycopg2', **db)


DB_POOL_FACTORY = {
    'mysql': make_mysql_dbpool,
    'pgsql': make_pgsql_dbpool,
    'sqlite': make_sqlite_dbpool,
}


def make_dbpool(db):
    db = dict(db)
    driver = db.pop('driver')
    return DB_POOL_FACTORY[driver](db)


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
