# coding: utf8

import functools
import collections

import logging
logger = logging.getLogger(__name__)

__all__ = ['SQL', 'DBUsingMixin', 'single_row', 'single_value']


def pure_db_operation(f):

    try:
        f._twoost_pure_db_operation = True
    except AttributeError:
        pass
    else:
        return f

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    wrapper._twoost_pure_db_operation = True
    return wrapper


def is_pure_db_operation(f):
    return getattr(f, '_twoost_pure_db_operation', False)


class SQL(tuple):

    """Composable pair of SQL string and attached args (tuple).

    Can be easily constructed/joined. Inherited from `tuple`.

    >>> SQL("SELECT * FROM table WHERE id = %s", 12345)
    SQL('SELECT * FROM table WHERE id = %s', 12345)

    >>> SQL("id = %s", 12345) + " AND " + SQL("name = %s", "Me")
    SQL('id = %s AND name = %s', 12345, 'Me')

    >>> list(SQL("SELECT * FROM table WHERE id = %s", 12345) + SQL(" AND name = %s", "Me"))
    ['SELECT * FROM table WHERE id = %s AND name = %s', (12345, 'Me')]

    >>> SQL.make(\
            "SELECT * FROM table", "WHERE",\
            SQL(" AND ").join([SQL("id = %s", 12345), SQL("name = %s", "Me")]))
    SQL('SELECT * FROM table WHERE id = %s AND name = %s', 12345, 'Me')

    """

    def __new__(cls, sql, *args):
        if not args and isinstance(sql, cls):
            return sql
        assert isinstance(sql, basestring)
        return tuple.__new__(cls, (str(sql), args))

    @classmethod
    def cast(cls, sql):
        if isinstance(sql, SQL):
            return sql
        elif isinstance(sql, tuple):
            s, a = sql
            return cls(sql, *a)
        elif isinstance(sql, basestring):
            return cls(sql)
        else:
            raise ValueError("invalid sql value", sql)

    @classmethod
    def make(cls, *sqls):
        return SQL(" ").join(sqls)

    def join(self, sqls):
        if not sqls:
            return SQL("")
        acc = sqls[0]
        for s in sqls[1:]:
            acc = acc + self + s
        return acc

    def __add__(self, other):
        s1, a1 = self
        s2, a2 = self.cast(other)
        return SQL((s1 + s2), *(a1 + a2))

    def __radd__(self, other):
        return self.cast(other) + self

    def __repr__(self):
        sql, args = self
        return "SQL{0!r}".format((sql,) + args)

    @pure_db_operation
    def execute(self, txn):
        sql, args = self
        txn.execute(sql, args)

    @pure_db_operation
    def fetch_all(self, txn):
        self.execute(txn)
        return txn.fetchall()

    @pure_db_operation
    def fetch_one(self, txn):
        self.execute(txn)
        row = txn.fetchone()
        if txn.fetchone() is not None:
            raise RuntimeError("expected 1 row")
        return row

    @pure_db_operation
    def fetch_single(self, txn):
        row = self.fetch_one(txn)
        return _single_coll_element(row)


def _single_coll_element(row):
    if row is None:
        return
    if len(row) != 1:
        raise RuntimeError("expected one column")
    if isinstance(row, collections.Mapping):
        return next(iter(row.values()))
    else:
        return next(iter(row))


def single_row(rows):
    if not rows:
        return None
    elif len(rows) == 1:
        return rows[0]
    else:
        raise RuntimeError("expected 1 row, provided multiple rows", len(rows))


def single_value(rows):
    row = single_row(rows)
    return _single_coll_element(row)


class DBUsingMixin(object):

    db_default = 'default'

    def __init__(self, dbs, *args, **kwargs):
        self.dbs = dbs

    def db_fetch_all(self, *sql_and_args, **kwargs):
        return self.db_run(SQL(*sql_and_args).fetch_all, **kwargs)

    def db_fetch_one(self, *sql_and_args, **kwargs):
        return self.db_run(SQL(*sql_and_args).fetch_one, **kwargs)

    def db_fetch_single(self, *sql_and_args, **kwargs):
        return self.db_run(SQL(*sql_and_args).fetch_single, **kwargs)

    def db_execute(self, *sql_and_args, **kwargs):
        return self.db_run(SQL(*sql_and_args).execute, **kwargs)

    def db_run(self, fn, *args, **kwargs):
        db = kwargs.pop('db', None) or self.db_default
        return self.dbs[db].runInteraction(fn, *args, **kwargs)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
