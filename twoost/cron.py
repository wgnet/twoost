# coding: utf-8

import crontab

from twisted.internet import defer, reactor
from twisted.application import service

from twoost import timed

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'CrontabTimerService',
]


def doxxo():
    def cancel(_):
        print("CAAANCEL...")
        d.errback(Exception("CANCELLED"))

    def after(_):
        print("done")
        d.callback('ok')

    print("Run D..")
    d = defer.Deferred(cancel)
    timed.sleep(30).addCallback(after)
    return d


class _PeriodicalDelayedCallService(service.Service):

    cancel_timeout = 60

    def __init__(self, callable, *args, **kwargs):
        self.clock = reactor
        self._callable = lambda: defer.maybeDeferred(callable, *args, **kwargs)
        self._call = None
        self._adef = None

    def _reschedule(self, now=False):
        if self._call and self._call.active():
            self._call.cancel()
        if self.running:
            t = self.nextDelay()
            self._call = self.clock.callLater(t, self.doCall)

    def doCall(self):
        self._adef = self._callable().addBoth(self._finish_call)

    def _finish_call(self, v):
        self._call = None
        self._adef = None
        self._reschedule()
        return v

    def nextDelay(self):
        raise NotImplementedError

    def startService(self):
        service.Service.startService(self)
        self._reschedule()

    @defer.inlineCallbacks
    def stopService(self):

        yield defer.maybeDeferred(service.Service.stopService, self)

        if self._call and self._call.active():
            self._call.cancel()

        if self._adef and not self._adef.called:
            try:
                timed.timeoutDeferred(self._adef, self.cancel_timeout)
                self._adef.cancel()
                yield self._adef
            except defer.CancelledError:
                logger.debug("deffered %r cancelled", self._adef)
            except Exception:
                logger.exception("cancellation error")


class CrontabTimerService(_PeriodicalDelayedCallService):

    next_previous_advance = 0.1

    def __init__(self, cronline, callable, *args, **kwargs):
        _PeriodicalDelayedCallService.__init__(self, callable, *args, **kwargs)
        self.cronline = cronline
        self.crontab = crontab.CronTab(cronline)

    def nextDelay(self):
        p = self.crontab.previous()
        n = self.crontab.next()
        ct = self.clock.seconds()
        if abs(ct - p) > self.next_previous_advance * abs(ct - n):
            logger.debug("crontab %r - ptime %s, ttime %s, use prev", self.cronline, p, n)
            return p
        else:
            logger.debug("crontab %r - ptime %s, ttime %s, use prev", self.cronline, p, n)
            return n


class IntervalTimerService(_PeriodicalDelayedCallService):

    _first_run = True

    def __init__(self, interval, callable, *args, **kwargs):
        _PeriodicalDelayedCallService.__init__(self, callable, *args, **kwargs)
        self.interval = float(interval)

    def startService(self):
        self._expect_call_at = self.clock.seconds()
        self._first_run = True
        return _PeriodicalDelayedCallService.startService(self)

    def nextDelay(self):

        if self._first_run:
            self._first_run = False
            return 0

        cur_time = self.clock.seconds()
        until_next_time = (self._expect_call_at - cur_time) % self.interval
        next_time = max(self._expect_call_at + self.interval, cur_time + until_next_time)

        if next_time == cur_time:
            next_time += self.interval

        self._expect_call_at = next_time
        return next_time - cur_time
