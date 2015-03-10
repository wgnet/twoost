# coding: utf-8

import time
import random

from twisted.internet import defer
from twoost import web, timed

import logging
logger = logging.getLogger(__name__)


# various web resources

class SlowHelloWorldResource(web.Resource):

    @defer.inlineCallbacks
    def render_GET(self, request):

        t1 = time.time()

        delay = float(request.args.get('delay', [random.random()])[0])
        yield timed.sleep(delay)

        dt = (time.time() - t1) * 1000

        request.setHeader('content-type', 'text/plain')
        defer.returnValue("date: %s\nrendered in %d ms" % (time.ctime(), dt))

# TODO: add more examples
