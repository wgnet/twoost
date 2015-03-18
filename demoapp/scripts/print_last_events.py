# coding: utf-8

from __future__ import print_function

import sys
import argparse
import datetime

from twisted.application import service
from twisted.internet import defer

from twoost.app import react_app, build_dbs, attach_service
from twoost.log import setup_script_logging

from demoapp.app import init_demoapp_settings
from demoapp.dao import DBDaoService

import logging
logger = logging.getLogger(__name__)


@defer.inlineCallbacks
def print_events(opts, dao):
    events = yield dao.all_events_after_dt(opts.from_dt)
    for e in events:
        print(">>", dict(e))


def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'from_dt',
        nargs='?',
        type=datetime.date,
        default=(datetime.datetime.now() - datetime.timedelta(days=3)),
    )
    opts = parser.parse_args(args)

    logger.info("start, args %r", args)
    app = service.Application(__name__)
    dbs = build_dbs(app)
    dao = attach_service(app, DBDaoService(dbs=dbs))

    react_app(app, print_events, (opts, dao))


if __name__ == '__main__':
    init_demoapp_settings()
    setup_script_logging()
    main(sys.argv[1:])
