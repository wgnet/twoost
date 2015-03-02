# coding: utf8

import types
import os
import uuid
import inspect
import collections

from zope import interface
from twisted.python import components


__all__ = [
    'settings',
    'load_conf_py',
    'lazy_prop',
    'alter_prop',
    'dynamic_prop',
    'IConfigProvider',
    'Config',
]


def lazy_prop(fn0):
    assert len(inspect.getargspec(fn0).args) == 0
    return _magic_prop(
        lambda root_config, prop_name, prev_value: fn0())


def alter_prop(fn1):
    assert len(inspect.getargspec(fn1).args) == 1
    return _magic_prop(
        lambda root_config, prop_name, prev_value: fn1(prev_value))


def dynamic_prop(fn1):
    assert len(inspect.getargspec(fn1).args) == 1
    return _magic_prop(
        lambda root_config, prop_name, prev_value: fn1(root_config))


def load_conf_py(fname):
    import imp
    fname = os.path.abspath(os.path.expandvars(os.path.expanduser(fname)))
    if not os.path.exists(fname):
        raise IOError("config file %r not found" % fname)
    return imp.load_source(fname, fname)


# ---

_magic_prop = collections.namedtuple('_magic_prop', 'fn')


class IConfigProvider(interface.Interface):

    def get(name, default):
        """Read config setting, may return ANYTHING or raise LookupError"""

    def keys():
        """Return collection of available settings. Use it only for debugging!"""


@interface.implementer(IConfigProvider)
class Config(object):
    """Base class for class-based configs"""

    def __init__(self):
        self.__props = [
            k
            for k in dir(self)
            if k == k.upper()
        ]

    def get(self, name, default):
        if name in self.__props:
            return getattr(self, name, default)
        else:
            return default

    def keys(self):
        return self.__props


@interface.implementer(IConfigProvider)
class _ModuleAdapter(object):

    """Adapter module->IConfigProvider"""

    def __init__(self, module):
        self.module = module
        self._keys = frozenset(
            k
            for k in dir(module)
            if k == k.upper()
        )

    def get(self, name, default):
        if name in self._keys:
            return getattr(self.module, name)
        else:
            return default

    def keys(self):
        return self._keys

    def __repr__(self):
        return "<_ModuleAdapter {0!r}>".format(self.module)


components.registerAdapter(_ModuleAdapter, types.ModuleType, IConfigProvider)
interface.classImplements(dict, IConfigProvider)

# ---


class MergedConfig(object):

    def __init__(self, configs):

        self._cache = {}
        self._keys = set()
        self._configs = list(map(IConfigProvider, configs))

        for c in self._configs:
            self._keys.update(c.keys())

    def __getattr__(self, key):

        # -- cache
        try:
            return self._cache[key]
        except LookupError:
            if key not in self._keys:
                raise AttributeError("no config prop", key)

        # -- calculate prop
        nope = object()

        pv = None
        for conf in self._configs:
            v = conf.get(key, nope)
            if v is nope:
                pass
            elif isinstance(v, _magic_prop):
                pv = v.fn(self, key, pv)
            else:
                pv = v

        if pv is nope:
            self._keys.remove(key)
            raise AttributeError("no config prop", key)

        self._cache[key] = pv
        return pv

    def get(self, key, default=None):
        return getattr(self, key, default)

    def keys(self):
        return self._keys

    def __repr__(self):
        return "<MergedConfig: {0}>".format(self.__configs)


class StackedConfig(object):

    def __init__(self):
        self.__configs = []
        self.__reload()

    def __reload(self):
        self.__current = MergedConfig([x[1] for x in self.__configs])

    def add_config(self, config):
        """Append new subconfig."""
        cf = IConfigProvider(config)
        cid = uuid.uuid4().hex
        self.__configs.append((cid, cf))
        self.__reload()
        return cid

    def remove_config(self, conf_id):
        """Remove subconfig, use this method in unit-tests"""
        if all(x != conf_id for x, _ in self.__configs):
            return
        self.__configs = [x for x in self.__configs if x[0] != conf_id]
        self.__reload()

    def __getattr__(self, key):
        return getattr(self.__current, key)

    def get(self, key, default=None):
        return self.__current.get(key, default)

    def keys(self):
        return self.__current.keys()

    def __repr__(self):
        return "<StackedConfig: {0!r}>".format([x[1] for x in self.__configs])


def _init_default_settings():
    from twoost.default_settings import DefaultSettings
    settings.add_config(DefaultSettings())


settings = StackedConfig()
_init_default_settings()
del _init_default_settings
