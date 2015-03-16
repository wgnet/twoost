# coding: utf8

import types
import os
import uuid
import collections

from zope import interface
from twisted.python import components, reflect
from twoost._misc import dd_merge


__all__ = [
    'settings',
    'load_conf_py',
    'load_conf_obj',
    'load_conf_json',
    'load_conf_yaml',
    'prop_merge',
    'prop_lazy',
    'prop_alter',
    'prop_dynamic',
    'IConfigProvider',
    'Config',
]


def _coerce_conf_file(fn):
    fn = os.path.abspath(os.path.expandvars(os.path.expanduser(fn)))
    if not os.path.exists(fn):
        raise IOError("config file %r not found" % fn)
    return fn


def load_conf_py(fname):
    import imp
    fname = _coerce_conf_file(fname)
    return imp.load_source(fname, fname)


def load_conf_obj(obj):
    if isinstance(obj, basestring):
        obj = reflect.namedAny(obj)
    return IConfigProvider(obj)


def load_conf_json(fname):
    import json
    with open(_coerce_conf_file(fname)) as f:
        return dict(json.load(f))


def load_conf_yaml(fname):
    import yaml
    with open(_coerce_conf_file(fname)) as f:
        return yaml.load(f)


def prop_lazy(fn, *args, **kwargs):
    assert callable(fn)
    return _prop_magic(
        lambda root_config, prop_name, prev_val_fn: fn(*args, **kwargs))


def prop_alter(fn, *args, **kwargs):
    assert callable(fn)
    return _prop_magic(
        lambda root_config, prop_name, prev_val_fn: fn(prev_val_fn(), *args, **kwargs))


def prop_dynamic(fn, *args, **kwargs):
    assert callable(fn)
    return _prop_magic(
        lambda root_config, prop_name, prev_val_fn: fn(root_config, *args, **kwargs))


def prop_merge(data):
    return _prop_magic(
        lambda root_config, prop_name, prev_val_fn: dd_merge(prev_val_fn(), data))


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
        self.__keys = frozenset(
            k
            for k in dir(module)
            if k == k.upper()
        )

    def get(self, name, default=None):
        if name in self.__keys:
            return getattr(self.module, name)
        else:
            return default

    def keys(self):
        return self.__keys

    def __repr__(self):
        return "<_ModuleAdapter {0!r}>".format(self.module)


components.registerAdapter(_ModuleAdapter, types.ModuleType, IConfigProvider)
interface.classImplements(dict, IConfigProvider)


# --- impl

# boxed fn for lazy/dynamic properties
_prop_magic = collections.namedtuple('_prop_magic', 'fn')


class ImmutableSettings(object):

    def __init__(self, configs):

        self.__cache = {}
        self.__keys = set()
        self.__configs = list(map(IConfigProvider, configs))

        for c in self.__configs:
            self.__keys.update(c.keys())

    def __calc_prop(self, key):

        nope = object()
        pit = iter(reversed(self.__configs))

        def prev_val_fn():

            for conf in pit:
                v = conf.get(key, nope)
                if v is nope:
                    pass
                elif isinstance(v, _prop_magic):
                    return v.fn(self, key, prev_val_fn)
                else:
                    return v
            else:
                self.__keys.remove(key)
                raise AttributeError("no config prop", key)

        return prev_val_fn()

    def __getattr__(self, key):
        try:
            return self.__cache[key]
        except LookupError:
            if key not in self.__keys:
                raise AttributeError("no config prop", key)
        val = self.__cache[key] = self.__calc_prop(key)
        return val

    def get(self, key, default=None):
        return getattr(self, key, default)

    def keys(self):
        return self.__keys

    def __repr__(self):
        return "<ImmutableSettings: {0}>".format(self.__configs)


class Settings(object):

    def __init__(self):
        self.__configs = collections.OrderedDict()
        self.__reload()
        self.__cid = 0

    def __reload(self):
        self.__current = ImmutableSettings(self.__configs.values())

    def add_config(self, config):
        """Append new subconfig."""
        cf = IConfigProvider(config)
        cid = uuid.uuid4().hex
        self.__configs[cid] = cf
        self.__reload()
        return cid

    def remove_config(self, conf_id):
        """Remove subconfig, use this method in unit-tests"""
        try:
            del self.__configs[conf_id]
        except KeyError:
            raise ValueError("unknown conf id", conf_id)
        self.__reload()

    def __getattr__(self, key):
        return getattr(self.__current, key)

    def get(self, key, default=None):
        return self.__current.get(key, default)

    def keys(self):
        return self.__current.keys()

    def __repr__(self):
        return "<Settings: {0!r}>".format(list(self.__configs.values()))


def _init_default_settings():
    from twoost.default_settings import DefaultSettings
    settings.add_config(DefaultSettings())


settings = Settings()
_init_default_settings()
del _init_default_settings
