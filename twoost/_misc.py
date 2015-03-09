# coding: utf-8

import re
import os
import copy
import errno
import functools
import itertools
import collections

import zope.interface

from twisted.python import log
from twisted.internet import defer


class TheProxy(object):

    def __init__(self, original):
        self.__original = original
        for ifc in zope.interface.providedBy(original):
            zope.interface.directlyProvides(self, ifc)

    def __getattr__(self, item):
        return getattr(self.__original, item)

    def __repr__(self):
        return '<%s wrapping %r>' % (self.__class__.__name__, self.__original)


class lazycol(object):

    __slots__ = ('_iterable',)

    def __new__(cls, _iterable):
        if isinstance(_iterable, (tuple, frozenset, lazycol)):
            return _iterable
        lc = object.__new__(cls)
        lc._iterable = _iterable
        return lc

    def __iter__(self):
        self._iterable, result = itertools.tee(self._iterable)
        return result


_DIGITS_RE = re.compile(r"([0-9]+)")


def natural_sorted(iterable):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in _DIGITS_RE.split(key)]
    return sorted(iterable, key=alphanum_key)


@property
def required_attr(self):
    raise NotImplementedError


class cached_property(object):

    __miss = object()

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, self.__miss)
        if value is self.__miss:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


def ignore_errors(f, logger=None):

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return defer.maybeDeferred(f, *args, **kwargs).addErrback(log.err)

    return wrapper


def subdict(d, keys=None):
    d = dict(d)
    if keys is None:
        return d
    return dict(
        (k, v)
        for k, v in d.items()
        if k in keys
    )


def merge_dicts(ds):
    d = {}
    for x in ds:
        d.update(x)
    return d


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def dd_merge(a, b):
    """merges `b` into `a` and return merged result"""

    if isinstance(a, collections.MutableSequence):
        if isinstance(b, collections.Sequence):
            a = copy.copy(a)
            a.extend(b)
            return a
        else:
            raise ValueError("cannot merge non-list into list", a, b)

    if isinstance(a, collections.MutableSet):
        if isinstance(b, collections.Set):
            a = copy.copy(a)
            a.update(b)
            return a
        else:
            raise ValueError("cannot merge non-set into set", a, b)

    if isinstance(a, collections.MutableMapping):
        if isinstance(b, collections.Mapping):
            a = copy.copy(a)
            for key in b:
                if key in a:
                    a[key] = dd_merge(a[key], b[key])
                else:
                    a[key] = b[key]
            return a
        else:
            raise ValueError("cannot merge non-dict into dict", a, b)

    else:
        return b

