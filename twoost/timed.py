# coding: utf-8

import functools

from twisted.internet import defer, reactor, task
from twisted.python import failure

__all__ = [
    'sleep',
    'withTimeout',
    'withParallelLimit',
    'TimeoutError',
]


class TimeoutError(defer.CancelledError):
    pass


def timeoutDeferred(d, timeout=120):

    if timeout is None:
        return d

    cancelled = [False]

    def do_cancel():
        if not d.called:
            cancelled[0] = True
            d.cancel()

    def cancel_canceller(x):
        if cancel_call.active():
            cancel_call.cancel()
        return x

    def convert_ce_to_te(x):
        if cancelled[0] and x.check(defer.CancelledError):
            return failure.Failure(TimeoutError(x))
        else:
            return x

    cancel_call = reactor.callLater(timeout, do_cancel)

    return d.addBoth(cancel_canceller).addErrback(convert_ce_to_te)


def withTimeout(seconds):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            d = defer.maybeDeferred(fn, *args, **kwargs)
            return timeoutDeferred(d, seconds)
        return wrapper
    return decorator


def withParallelLimit(limit, timeout=120):

    def decorator(fn):
        if timeout is not None:
            fn = withTimeout(timeout)(fn)
        if not limit:
            return fn
        semaphore = defer.DeferredSemaphore(limit)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # use `deferLater` to avoid recursion overflow
            return semaphore.run(task.deferLater, reactor, 0, fn, *args, **kwargs)

        return wrapper

    return decorator


def sleep(time):
    return task.deferLater(reactor, time, int)
