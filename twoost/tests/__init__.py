# utf-8

from twisted.internet import defer
import logging

if 0:

    from twisted.python import log
    observer = log.PythonLoggingObserver()
    observer.start()

    defer.setDebugging(True)

    import twisted.internet.base
    twisted.internet.base.DelayedCall.debug = True
    logging.basicConfig(level=logging.DEBUG)

else:
    logging.getLogger('twoost.amqp').setLevel(logging.FATAL)
    logging.getLogger('pika').setLevel(logging.FATAL)
