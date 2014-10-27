# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from twoost import timed


class TimeoutTest(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_no_timeout(self):
        d = defer.succeed(1)
        timed.timeoutDeferred(d).addCallback(self.assertEqual, 1)

    @defer.inlineCallbacks
    def test_timeout_deferred(self):
        d = timed.sleep(10000).addCallback(lambda _: 1)
        try:
            yield timed.timeoutDeferred(d, timeout=0.1)
        except timed.TimeoutError:
            pass
        else:
            self.fail("Expected TimeoutError")

    @defer.inlineCallbacks
    def test_timeout(self):
        try:
            yield timed.withTimeout(0.1)(timed.sleep)(10000)
        except timed.TimeoutError:
            pass
        else:
            self.fail("Expected TimeoutError")

    @defer.inlineCallbacks
    def test_limit_call_rate(self):

        self.cnt = 0
        self.max_cnt = 0

        @defer.inlineCallbacks
        def call():
            self.cnt += 1
            self.max_cnt = max(self.max_cnt, self.cnt)
            yield timed.sleep(0.05)
            self.cnt -= 1

        xcall = timed.withParallelLimit(3)(call)
        yield defer.gatherResults([xcall() for _ in range(20)])

        self.assertEqual(0, self.cnt)
        self.assertEqual(3, self.max_cnt)
