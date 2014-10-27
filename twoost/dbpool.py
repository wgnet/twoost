# coding: utf-8

import os

from twisted.enterprise.adbapi import ConnectionPool
from twisted.application import service

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'DatabaseService',
    'make_dbpool',
]


try:
    import sqlite3
except ImportError:
    logger.debug("SqLite driver not found")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    logger.debug("PgSQL driver not found")

try:
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    logger.debug("MySQL driver not found")


# ---

class _PGSqlConnectionPool(ConnectionPool):

    def __init__(self, *args, **kwargs):
        ConnectionPool.__init__(self, *args, cursor_factory=psycopg2.extras.DictCursor, **kwargs)

    def _runInteraction(self, interaction, *args, **kw):
        try:
            return ConnectionPool._runInteraction(self, interaction, *args, **kw)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning("resetting DB db_pooll due to %r", e)
            for conn in self.connections.values():
                self._close(conn)
            self.connections.clear()
            return ConnectionPool._runInteraction(self, interaction, *args, **kw)

    def prepare_connection(self, connection):
        psycopg2.extras.register_hstore(connection)

    def connect(self):
        old_connections = self.connections
        conn = ConnectionPool.connect(self)
        new_connection = conn not in old_connections
        if new_connection:
            self.prepare_connection(conn)
        return conn


class _MySQLConnectionPool(ConnectionPool):
    """Reconnecting adbapi connection pool for MySQL.

    This class improves on the solution posted at
    http://www.gelens.org/2008/09/12/reinitializing-twisted-connectionpool/
    by checking exceptions by error code and only disconnecting the current
    connection instead of all of them.

    Also see:
    http://twistedmatrix.com/pipermail/twisted-python/2009-July/020007.html

    """

    def __init__(self, *args, **kwargs):
        ConnectionPool.__init__(self, *args, cursorclass=MySQLdb.cursors.DictCursor, **kwargs)

    def _runInteraction(self, interaction, *args, **kwargs):
        try:
            return ConnectionPool._runInteraction(self, interaction, *args, **kwargs)
        except MySQLdb.OperationalError as e:
            logger.error("ERROR, %r, type %s", e, type(e))
            if e[0] not in (2006, 2013):
                raise
            conn = self.connections.get(self.threadID())
            self.disconnect(conn)
            # try the interaction again
            return ConnectionPool._runInteraction(self, interaction, *args, **kwargs)


class _SQLiteConnectionPool(ConnectionPool):

    def connect(self):
        conn = ConnectionPool.connect(self)
        conn.row_factory = sqlite3.Row
        return conn


# ---

def make_sqlite_dbpool(db):

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
    return _MySQLConnectionPool('MySQLdb', **db)


def make_pgsql_dbpool(db):

    db = dict(db)
    db.setdefault('cp_reconnect', True)
    db.setdefault('host', 'localhost')
    db.setdefault('port', 5432)

    logger.debug("connecting to pgsql %r", db['database'])
    return _PGSqlConnectionPool('psycopg2', **db)


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
