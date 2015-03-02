# coding: utf-8

from __future__ import print_function, division, absolute_import

import os

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from twoost import dbpool
from twoost._misc import mkdir_p


class SqlitePoolTest(TestCase):

    def setUp(self):
        td = os.environ['TEST_TMP_DIR'] = self.mktemp()
        mkdir_p(td)

    @defer.inlineCallbacks
    def test_sqlite3_driver(self):

        db = dbpool.make_dbpool({
            'driver': 'sqlite',
            'database': "$TEST_TMP_DIR/test.db",
        })

        yield db.runOperation("CREATE TABLE twoost_the_table (x, y)")
        yield db.runOperation("INSERT INTO twoost_the_table (x, y) VALUES (?, ?)", [1, 2])
        yield db.runOperation("INSERT INTO twoost_the_table (x, y) VALUES (?, ?)", [10, 20])
        rows = yield db.runQuery("SELECT x FROM twoost_the_table")
        self.assertEquals([{'x': 1}, {'x': 10}], map(dict, rows))

        def delete_rows(txn):
            txn.execute("DELETE FROM twoost_the_table")

        yield db.runInteraction(delete_rows)
        rows = yield db.runQuery("SELECT x FROM twoost_the_table")
        self.assertEquals([], rows)

        yield db.runOperation("DROP TABLE twoost_the_table")

        db.close()

    def test_db_service(self):

        dbs = dbpool.DatabaseService({
            'db1': {'driver': 'sqlite', 'database': "$TEST_TMP_DIR/dbs_1.db"},
            'db2': {'driver': 'sqlite', 'database': "$TEST_TMP_DIR/dbs_2.db"},
            'db3': {'driver': 'sqlite', 'database': "$TEST_TMP_DIR/dbs_3.db"},
        })
        dbs.startService()

        self.assertEquals(3, len(list(dbs)))
        self.assertIsInstance(dbs['db1'], dbpool.SQLiteConnectionPool)
        self.assertIsInstance(dbs['db2'], dbpool.SQLiteConnectionPool)
        self.assertIsInstance(dbs['db3'], dbpool.SQLiteConnectionPool)

        dbs.stopService()


class PGDbPoolTest(TestCase):

    @defer.inlineCallbacks
    def test_pgsql_driver(self):

        db = dbpool.make_dbpool({
            'driver': 'pgsql',
            'database': 'test',
            'user': 'test',
            'password': 'test',
        })

        yield db.runOperation("DROP TABLE IF EXISTS twoost_the_table")
        yield db.runOperation("CREATE TABLE twoost_the_table (x int, y int)")

        yield db.runOperation("INSERT INTO twoost_the_table (x, y) VALUES (%s, %s)", [1, 2])
        yield db.runOperation("INSERT INTO twoost_the_table (x, y) VALUES (%s, %s)", [10, 20])

        rows = yield db.runQuery("SELECT x FROM twoost_the_table")
        self.assertEquals([{'x': 1}, {'x': 10}], map(dict, rows))

        def delete_rows(txn):
            txn.execute("DELETE FROM twoost_the_table")

        yield db.runInteraction(delete_rows)
        rows = yield db.runQuery("SELECT x FROM twoost_the_table")
        self.assertEquals([], rows)

        yield db.runOperation("DROP TABLE IF EXISTS twoost_the_table")
        db.close()
