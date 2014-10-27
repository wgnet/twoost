# coding: utf-8
# twoost documentation build configuration file

import sys
import os

sys.path.insert(0, os.path.abspath('../'))
# needs_sphinx = '1.0'

extensions = []
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'Twoost'
copyright = u'2014, WG'

exclude_patterns = ['_build']
html_static_path = ['_static']
pygments_style = 'sphinx'
html_theme = 'default'
html_static_path = []
htmlhelp_basename = 'twoost-doc'

# (source start file, name, description, authors, manual section).
man_pages = [('index', 'twoost', u'Twoost documentation', [], 1)]
