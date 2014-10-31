#!twistd -ny

import os
from twisted.python.reflect import namedAny

geninit_ctor_name = os.environ['TWOOST_GENINIT_CTOR']
geninit_ctor = namedAny(geninit_ctor_name)
application = geninit_ctor().create_twisted_application()
