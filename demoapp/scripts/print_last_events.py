# coding: utf-8

from __future__ import print_function

import sys
import argparse
import datetime

from twisted.application import service
from twisted.internet import defer

from twoost.log import setup_script_logging

from demoapp import settings
from demoapp.app import *  # noqa
from demoapp.storage import DBDaoService


import logging
logger = logging.getLogger(__name__)


@defer.inlineCallbacks
def print_events(args, dao):
    dt = args.from_dt or datetime.datetime.now() - datetime.timedelta(days=3)
    events = yield dao.all_events_after_dt(dt)
    for e in events:
        print(">>", repr(e) if settings.DEBUG else e)


def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument('from_dt', type=datetime.date)
    args = parser.parse_args(raw_args)

    logger.info("start, args %r", args)
    app = service.Application(__name__)
    dbs = build_dbs(app)
    dao = attach_service(app, DBDaoService(dbs=dbs))

    return react_app(app, print_events, args, dao)


if __name__ == '__main__':
    setup_script_logging()
    main(sys.argv[1:])
