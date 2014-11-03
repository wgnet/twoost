# coding: utf-8

__version__ = '0.1.dev0'


def _init_default_settings():

    from twoost.default_settings import DefaultSettings
    from twoost.log import LoggingSettings
    from twoost.conf import settings

    settings.add_config(DefaultSettings())
    settings.add_config(LoggingSettings())


_init_default_settings()
del _init_default_settings
