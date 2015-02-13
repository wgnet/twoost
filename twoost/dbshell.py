# coding: utf-8

from __future__ import print_function

import argparse
import subprocess
import sys
import os
import tempfile

from twoost.conf import settings


__all__ = ['run_dbshell']


def run_pgsql_dbshell(db):
    # tempfile always use 0600 - great!
    with tempfile.NamedTemporaryFile() as tf:
        return _run_pgsql_dbshell_with_tf(db, tf)


def _run_pgsql_dbshell_with_tf(db, tf):

    db = dict(db)
    args = ['psql']
    env = os.environ.copy()
    env['PGPASSFILE'] = tf.name

    database = db.get('database')
    user = db.get('user')
    password = db.get('password')
    host = db.get('host')
    port = db.get('port')

    if user:
        args.append("-U")
        args.append(user)
    if host:
        args.append("-h")
        args.append(host)
    if port:
        args.append("-p")
        args.append(str(port))
    if database:
        args.append(database)

    if password:
        quote = lambda x: x.replace("\\", "\\\\").replace(":", "\\:") if x else "*"
        tf.write("{host}:*:{db}:{user}:{password}\n".format(
            host=quote(host),
            db=quote(database),
            user=quote(user),
            password=quote(password),
        ))
        tf.flush()

    return subprocess.call(args, env=env)


def run_mysql_dbshell(db):
    with tempfile.NamedTemporaryFile() as tf:
        return _run_mysql_dbshell_with_tf(db, tf)


def _run_mysql_dbshell_with_tf(db, tf):

    db = dict(db)

    database = db.get('database') or db.get('db')
    user = db.get('user')
    passwd = db.get('password') or db.get('passwd')
    host = db.get('host')
    port = db.get('port')

    args = ["mysql"]
    args.appent("--defaults-file=%s" % tf.name)

    if user:
        args.append("--user=%s" % user)
    # if passwd:
    #     args.append("--password=%s" % passwd)
    if host:
        if '/' in host:
            args.append("--socket=%s" % host)
        else:
            args.append("--host=%s" % host)
    if port:
        args.append("--port=%s" % port)
    if database:
        args.append(database)
    if passwd:
        tf.write("[client]\npassword=%s\n" % passwd)
        tf.flush()

    return subprocess.call(args)


def run_sqlite_dbshell(db):
    db = dict(db)
    database = db.get('database')
    args = ["sqlite3", database]
    return subprocess.call(args)


DBSHELL_RUNNER = {
    'mysql': run_mysql_dbshell,
    'pgsql': run_pgsql_dbshell,
    'sqlite': run_sqlite_dbshell,
}


def run_dbshell(db):
    db = dict(db)
    driver = db.pop('driver')
    return DBSHELL_RUNNER[driver](db)


def create_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', '-d', default='default', help="database name")
    return parser


def main(argv=None):

    if argv is None:
        argv = sys.argv[1:]
    pa = create_argparser().parse_args(argv)
    database = pa.database

    if database not in settings.DATABASES:
        print(
            "Unknown db %r, available dbs: %r" % (database, settings.DATABASES.keys()),
            file=sys.stderr)
        sys.exit(1)

    return run_dbshell(settings.DATABASES[database])


if __name__ == '__main__':
    main()
