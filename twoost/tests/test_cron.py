# coding: utf-8

from __future__ import print_function, division, absolute_import

import datetime
import itertools

from twisted.application.service import Application, IServiceCollection
from twisted.internet import defer, task
from twisted.trial.unittest import TestCase

from twoost import cron, app


class IntervalTimerServiceTest(TestCase):

    def setUp(self):
        self.ticks = 0
        self.clock = task.Clock()

    def tearDown(self):
        self.assertFalse(self.clock.getDelayedCalls())

    @defer.inlineCallbacks
    def test_start_stop(self):

        def tick():
            self.ticks += 1

        s = cron.IntervalTimerService(0.105, tick)
        s.clock = self.clock

        self.clock.pump([0.01] * 100)
        self.assertEqual(0, self.ticks)

        s.startService()
        self.clock.pump([0.01] * 100)
        self.assertEqual(10, self.ticks)

        yield s.stopService()
        self.clock.pump([0.01] * 100)
        self.assertEqual(10, self.ticks)

        s.startService()
        self.clock.pump([0.01] * 100)
        self.assertEqual(20, self.ticks)

        yield s.stopService()
        self.clock.advance(10)
        self.assertEqual(20, self.ticks)

    @defer.inlineCallbacks
    def test_long_task(self):

        @defer.inlineCallbacks
        def long_tick():
            yield task.deferLater(self.clock, 1, int)
            self.ticks += 1

        s = cron.IntervalTimerService(0.01, long_tick)
        s.clock = self.clock

        s.startService()
        self.clock.pump([0.01] * 1050)
        self.assertEqual(10, self.ticks)

        yield s.stopService()
        self.clock.pump([0.01] * 1000)
        es = self.flushLoggedErrors(defer.CancelledError)
        self.assertEqual(1, len(es))
        self.assertEqual(11, self.ticks)

        self.clock.advance(10)
        self.assertEqual(11, self.ticks)

    @defer.inlineCallbacks
    def test_run_now(self):

        def tick():
            self.ticks += 1

        s = cron.IntervalTimerService(1000, tick)
        s.clock = self.clock

        s.startService()
        self.clock.advance(0)
        self.assertEqual(1, self.ticks)

        yield s.stopService()
        self.clock.advance(100)
        self.assertEqual(1, self.ticks)

    @defer.inlineCallbacks
    def test_bad_callback(self):

        def bad_tick():
            self.ticks += 1
            raise RuntimeError('bad_cron_callback')

        s = cron.IntervalTimerService(0.105, bad_tick)
        s.clock = self.clock

        s.startService()
        self.clock.pump([0.01] * 100)
        self.assertEqual(10, self.ticks)

        es = self.flushLoggedErrors(RuntimeError)
        self.assertTrue(all(e.value.args == ('bad_cron_callback',) for e in es))

        yield s.stopService()
        self.clock.pump([0.01] * 100)
        self.assertEqual(10, self.ticks)


class CronTimerServiceTest(TestCase):

    def setUp(self):
        self.ticks = 0
        self.clock = task.Clock()

    def tearDown(self):
        self.assertFalse(self.clock.getDelayedCalls())

    @defer.inlineCallbacks
    def test_start_stop(self):

        def tick():
            self.ticks += 1

        self.clock.rightNow = datetime.datetime(2015, 1, 1, 0, 0, 0).toordinal()

        # each minute
        s = cron.CrontabTimerService("0-59 * * * *", tick)
        s.clock = self.clock

        self.clock.advance(100)
        self.assertEqual(0, self.ticks)

        s.startService()
        self.clock.pump([1] * 100)
        self.assertEqual(2, self.ticks)

        s.startService()
        self.clock.pump([1] * 600)
        self.assertEqual(12, self.ticks)

        yield s.stopService()
        self.clock.advance(100)
        self.assertEqual(12, self.ticks, "no more ticks")

    @defer.inlineCallbacks
    def test_cronlines(self):

        def tick():
            self.ticks += 1

        minute = 60
        hour = 60 * minute
        day = 24 * hour
        year = 365 * day

        @defer.inlineCallbacks
        def dotest(cronline, time_period, ticks):

            self.ticks = 0
            self.clock.rightNow = datetime.datetime(2015, 1, 1, 0, 0, 1).toordinal()

            s = cron.CrontabTimerService(cronline, tick)
            s.clock = self.clock
            s.startService()

            count, delta = time_period
            self.clock.pump(itertools.islice(itertools.cycle([delta]), count))
            self.assertEqual(ticks, self.ticks)

            yield s.stopService()
            self.clock.advance(10 * year)
            self.assertEqual(ticks, self.ticks)

        yield dotest("15 14 1 * *", (100, day), 3)
        yield dotest("5 0 * * *", (240, hour), 10)
        yield dotest("50 19 * * *", (240, hour), 10)
        yield dotest("50 19 * * 1,3,5", (240, hour), 4)
        yield dotest("*/10 * * * *", (60, minute),  6)
        yield dotest("15 10,15 * * 1,4", (340, hour), 8)


class AppTimerServiceTest(TestCase):

    def setUp(self):
        self.app = Application('test_cron')
        self.tick = lambda: None

    def test_build_timer_cron(self):
        s1 = app.build_timer(self.app, "* 1 * 2 *", self.tick)
        self.assertIs(IServiceCollection(self.app), s1.parent)
        self.assertIsInstance(s1, cron.CrontabTimerService)
        self.assertEqual("* 1 * 2 *", s1.cronline)

        s2 = app.build_timer(self.app, ['*', '5-10', '*', '*', '*'], self.tick)
        self.assertIs(IServiceCollection(self.app), s1.parent)
        self.assertIsInstance(s1, cron.CrontabTimerService)
        self.assertEqual("* 5-10 * * *", s2.cronline)

    def test_build_interval(self):

        s1 = app.build_timer(self.app, 1234, self.tick)
        self.assertIs(IServiceCollection(self.app), s1.parent)
        self.assertIsInstance(s1, cron.IntervalTimerService)
        self.assertEqual(1234, s1.interval)

        s1 = app.build_timer(self.app, " 999 ", self.tick)
        self.assertIs(IServiceCollection(self.app), s1.parent)
        self.assertIsInstance(s1, cron.IntervalTimerService)
        self.assertEqual(999, s1.interval)
