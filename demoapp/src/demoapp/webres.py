# coding: utf-8

from twisted.internet import defer
from twoost import web, timed

import logging
logger = logging.getLogger(__name__)


# various web resources

class SlowHelloWorldResource(web.Resource):

    @defer.inlineCallbacks
    def render_GET(self, request):
        time = float(request.args.get('time', [1])[0])
        yield timed.sleep(time)
        defer.returnValue("Hello, world!")

# TODO: add more examples
