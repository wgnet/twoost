# coding: utf-8

__version__ = '0.0.1'

import os
from demoapp import default_settings
from twoost.conf import settings, load_conf_py


def init_settings():

    if 'CONF_DIR' not in os.environ:
        os.environ['CONF_DIR'] = os.path.expanduser("~/conf")

    settings.add_config(default_settings)
    settings.add_config(load_conf_py("$CONF_DIR/demoapp/settings.py"))
    # settings.add_config({'DEBUG': True'})
