# coding: utf8

import types
import os

from zope import interface
from twisted.python import components


__all__ = [
    'settings',
    'load_conf_py',
    'IConfig',
    'Config',
]


class IConfig(interface.Interface):

    def __getattr__(key):
        """Read config setting, may return ANYTHING or raise AttributeError"""

    def __dir__():
        """Return list of available settings. Use it only for debugging!"""


@interface.implementer(IConfig)
class Config(object):
    """Base class for class-based configs"""


# all modules already behave like config (settings.py pattern)
interface.classImplements(types.ModuleType, IConfig)


@interface.implementer(IConfig)
class DictConfig(object):

    """Adapter dict->IConfig"""

    def __init__(self, origin):
        self.__dict = dict(origin)

    def __getattr__(self, key):
        try:
            return self.__dict[key]
        except KeyError:
            raise AttributeError

    def __dir__(self):
        return list(self.__dict.keys())

    def __repr__(self):
        return "<DictConfig {0!r}>".format(self.__dict)

    def __eq__(self, other):
        return isinstance(other, DictConfig) and self.__dict == other.__dict

    def __ne__(self, other):
        return not self == other


components.registerAdapter(DictConfig, dict, IConfig)


# ---

@interface.implementer(IConfig)
class MergedConfig(object):

    def __init__(self, configs):
        self.__configs = list(reversed(configs))
        self.__dir = None
        self.__attr_error = set()

    def __getattr__(self, key):

        if key in self.__attr_error:
            raise AttributeError

        for c in self.__configs:
            try:
                v = getattr(c, key)
            except AttributeError:
                pass
            else:
                setattr(self, key, v)
                return v

        self.__attr_error.add(key)
        raise AttributeError

    def __dir__(self):
        if self.__dir is None:
            s = set()
            for c in self.__configs:
                s.update(dir(c))
            self.__dir = sorted(s)
        return self.__dir

    def __repr__(self):
        return "<MergedConfig: {0}>".format(self.__configs)


def load_conf_py(fname):
    import imp
    fname = os.path.abspath(os.path.expandvars(os.path.expanduser(fname)))
    if not os.path.exists(fname):
        raise IOError("config file %r not found" % fname)
    return imp.load_source(fname, fname)


# --- global settings

@interface.implementer(IConfig)
class ConfigProxy(object):

    def __init__(self):
        self.__current = None
        self.__configs = []

    def __getattr__(self, key):
        c = self.__current
        if c is None:
            raise AttributeError
        else:
            return getattr(c, key)

    def __dir__(self):
        c = self.__current
        if c is None:
            raise AttributeError
        return dir(c)

    def __reload(self):
        if self.__configs:
            self.__current = MergedConfig(self.__configs)
        else:
            self.__current = None

    def __repr__(self):
        return "<ConfigProxy: {0!r}>".format(self.__current)

    def add_config(self, config):
        """Append new subconfig."""
        if config is self:
            return
        c = IConfig(config)
        if c not in self.__configs:
            self.__configs.append(c)
            self.__reload()
        return self.__current

    def remove_config(self, config):
        """Remove subconfig, use this method in unit-tests"""
        c = IConfig(config)
        if c in self.__configs:
            self.__configs.remove(c)
            self.__reload()
        return self.__current

# ---


def _init_default_settings():
    from twoost.default_settings import DefaultSettings
    settings.add_config(DefaultSettings())


settings = ConfigProxy()
_init_default_settings()
del _init_default_settings
