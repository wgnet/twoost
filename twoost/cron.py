# coding: utf-8

import crontab

import zope.interface

from twisted.internet import defer, reactor
from twisted.application import service
from twisted.python import log

from twoost import timed, health

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'CrontabTimerService',
]


@zope.interface.implementer(health.IHealthChecker)
class _PeriodicalDelayedCallService(service.Service):

    cancelTimeout = 60
    handleFailure = staticmethod(log.err)

    def __init__(self, callback, *args, **kwargs):
        self.clock = reactor
        self.callback = callback
        self._lastCallFailure = None
        self._delayedCall = None
        self._activeDeferred = None

    def _reschedule(self):
        if self._delayedCall and self._delayedCall.active():
            logger.debug("cancel call %r", self._delayedCall)
            self._delayedCall.cancel()
        if self.running:
            t = self.nextDelay()
            logger.debug("call %r after %r secs", self.callback, t)
            self._delayedCall = self.clock.callLater(t, self.doCall)

    def doCall(self):
        assert self._activeDeferred is None
        self._activeDeferred = defer.maybeDeferred(self.callback)
        self._activeDeferred.addCallbacks(self._clearLastFailure, self._trackLastFailure)
        self._activeDeferred.addErrback(self.handleFailure)
        self._activeDeferred.addBoth(self._finishCall)

    def _trackLastFailure(self, f):
        self._lastCallFailure = f
        return f

    def _clearLastFailure(self, x):
        self._lastCallFailure = None
        return x

    def _finishCall(self, v):
        self._delayedCall = None
        self._activeDeferred = None
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

        if self._delayedCall and self._delayedCall.active():
            self._delayedCall.cancel()

        activeDeferred = self._activeDeferred
        if activeDeferred and not activeDeferred.called:
            try:
                timed.timeoutDeferred(activeDeferred, self.cancelTimeout, self.clock)
                activeDeferred.cancel()
                yield activeDeferred
            except defer.CancelledError:
                logger.debug("deffered %r cancelled", activeDeferred)
            except Exception:
                logger.exception("cancellation error")

    def checkHealth(self):
        if self._lastCallFailure:
            return False, self._lastCallFailure
        else:
            return True, ""


class CrontabTimerService(_PeriodicalDelayedCallService):

    def __init__(self, cronline, callable, *args, **kwargs):
        _PeriodicalDelayedCallService.__init__(self, callable, *args, **kwargs)
        self.cronline = cronline
        self.crontab = crontab.CronTab(self.cronline)

    def nextDelay(self):
        now = self.clock.seconds()
        return self.crontab.next(now)

    def __repr__(self):
        return "<CrontabTimerService: callback %r, cronline %r>" % (self.callback, self.cronline)


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
            logger.debug("%s: first run", self)
            self._first_run = False
            return 0

        cur_time = self.clock.seconds()
        until_next_time = (self._expect_call_at - cur_time) % self.interval
        next_time = max(self._expect_call_at + self.interval, cur_time + until_next_time)

        if next_time == cur_time:
            next_time += self.interval

        delay = next_time - cur_time
        self._expect_call_at = next_time

        logger.debug("%s: expect next call at %s, delay %s", self, next_time, delay)
        return delay

    def __repr__(self):
        return "<IntervalTimerService: callback %r, interval %r>" % (self.callback, self.interval)
