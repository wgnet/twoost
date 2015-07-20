# coding: utf-8

from __future__ import print_function, division, absolute_import

import time
import os
import sys
import logging
import errno

from stat import ST_DEV, ST_INO
from logging.handlers import TimedRotatingFileHandler

from twisted.internet import reactor, protocol
from twisted.python import log, reflect, lockfile

try:
    import raven
except ImportError:
    raven = None

from .conf import settings, Config


__all__ = [
    'setup_logging',
    'setup_script_logging',
]


_log_initialized = False


def _do_logging_config():
    log.msg("Initialize python logging")
    reflect.namedAny(settings.LOGGING_CONFIG_INIT)(settings.LOGGING)


def _redirect_twisted_log():
    log.msg("Redirect twisted log")
    log.startLoggingWithObserver(log.PythonLoggingObserver().emit, setStdout=0)


def setup_logging(appname):
    global _log_initialized
    assert not _log_initialized
    _log_initialized = True
    reactor.addSystemEventTrigger('after', 'shutdown', logging.shutdown)
    settings.add_config({
        'TWOOST_LOG_APPNAME': appname,
    })
    _do_logging_config()
    _redirect_twisted_log()


def setup_script_logging(debug=None):
    script_name = os.path.basename(sys.argv[0])
    settings.add_config({
        'TWOOST_LOG_TEE_TO_CONSOLE': True,
    })
    if debug is not None:
        settings.add_config({'DEBUG': debug})
    setup_logging(appname=script_name)


class LoggingSettings(Config):

    TWOOST_LOG_APPNAME = "-"
    TWOOST_LOG_LEVEL = None
    TWOOST_LOG_TEE_TO_CONSOLE = None

    LOG_INTERVAL = 'D'
    LOG_DIR = os.environ.get("TWOOST_LOG_DIR") or os.path.expanduser("~/logs")

    @property
    def LOG_LEVEL(self):
        return logging.DEBUG if settings.DEBUG else logging.INFO

    @property
    def TWOOST_LOG_NAME(self):
        return settings.TWOOST_LOG_APPNAME + ".log"

    @property
    def LOGGING(self):

        level = settings.LOG_LEVEL
        log_interval = settings.LOG_INTERVAL
        workerid = settings.TWOOST_LOG_APPNAME

        log_dir = settings.LOG_DIR
        log_file = os.path.join(log_dir, settings.TWOOST_LOG_NAME)

        conf = {
            'version': 1,
            'disable_existing_loggers': False,
            'handlers': {
                'null': {
                    'class': 'logging.NullHandler',
                },
                'console': {
                    'level': level,
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
                'filelog': {
                    'level': level,
                    'class': 'twoost.log.TimedRotatingFileHandlerSafe',
                    'filename': log_file,
                    'when': log_interval,
                    'formatter': 'verbose',
                },
                'mail_admins': {
                    'level': 'CRITICAL',
                    'class': 'twoost.log.AdminEmailHandler'
                }
            },
            'formatters': {
                'verbose': {
                    'format': "[%(asctime)s] [%(levelname)s] [{0}/%(process)d] "
                    "[%(name)s]:  %(message)s".format(workerid),
                },
                'simple': {
                    'format': "%(asctime)s %(message)s",
                    'datefmt': "%H:%M:%S",
                },
            },
            'loggers': {
                'pika': {'level': 'WARNING'},
                'twoost': {'level': settings.TWOOST_LOG_LEVEL or level},
                'py.warnings': {'handlers': ['console']},
            },
            'root': {
                'handlers': ['filelog'],
                'level': level,
            },
        }

        sentry_dsn = settings.SENTRY_DSN
        if raven and sentry_dsn:
            conf['handlers']['sentry'] = {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': sentry_dsn,
            }
            conf['root']['handlers'].append('sentry')

        if settings.TWOOST_LOG_TEE_TO_CONSOLE:
            conf['root']['handlers'].append('console')

        return conf


# -- log handlers

class TimedRotatingFileHandlerSafe(TimedRotatingFileHandler):

    """
    Combination of TimedRotatingFileHandler & WatchedFileHandler.

    Several workers can write to the same log without _horrible_ consequences.
    Through, spurious logging artefacts still possible.
    """

    _lockf = None

    def __init__(self, *args, **kwargs):
        TimedRotatingFileHandler.__init__(self, *args, **kwargs)
        self.dev, self.ino = -1, -1
        self._statstream()
        self._flock = lockfile.FilesystemLock(self.baseFilename + "~lock")

    # stolen from standart WatchedFileHandler

    def _statstream(self):
        if self.stream:
            sres = os.fstat(self.stream.fileno())
            self.dev, self.ino = sres[ST_DEV], sres[ST_INO]

    def emit(self, record):
        """
        Emit a record.

        First check if the underlying file has changed, and if it
        has, close the old stream and reopen the file to get the
        current stream.
        """
        # Reduce the chance of race conditions by stat'ing by path only
        # once and then fstat'ing our new fd if we opened a new log stream.
        # See issue #14632: Thanks to John Mulligan for the problem report
        # and patch.
        try:
            # stat the file by path, checking for existence
            sres = os.stat(self.baseFilename)
        except OSError as err:
            if err.errno == errno.ENOENT:
                sres = None
            else:
                raise
        # compare file system stat with that of our stream file handle
        if not sres or sres[ST_DEV] != self.dev or sres[ST_INO] != self.ino:
            if self.stream is not None:
                # we have an open file handle, clean it up
                self.stream.flush()
                self.stream.close()
                # open a new file handle and get new stat info from that fd
                self.stream = self._open()
                self._statstream()
        TimedRotatingFileHandler.emit(self, record)

    # stolen from standart TimedRotatingFileHandler

    def _actual_doRollover(self):  # noqa
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)

        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)

        # WE DON'T DELETE dfn WHEN IT ALREADY EXISTS!
        if not os.path.exists(dfn) and os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)

        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)

        self.mode = 'a'
        self.stream = self._open()

        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval

        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:           # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend

        self.rolloverAt = newRolloverAt

    # protect `rolover` with file-lock

    def doRollover(self):
        if self._flock.lock():
            try:
                self._actual_doRollover()
            finally:
                self._flock.unlock()


class AdminEmailHandler(logging.Handler):
    """An exception log handler that emails log entries to site admins. """

    def emit(self, record):

        subject = "%s: %s %s" % (
            record.levelname,
            settings.TWOOST_LOG_APPNAME,
            record.getMessage())
        subject = self._format_subject(subject)
        message = self.format(record)

        from twoost import email
        email.send_mail(
            subject, message,
            settings.EMAIL_DEFAULT_FROM,
            [s[1] for s in settings.ADMINS],
        )

    def _format_subject(self, subject):
        """
        Escape CR and LF characters, and limit length.
        RFC 2822's hard limit is 998 characters per line. So, minus "Subject: "
        the actual subject must be no longer than 989 characters.
        """
        formatted_subject = subject.replace('\n', '\\n').replace('\r', '\\r')
        return formatted_subject[:989]


if raven and hasattr(raven.transport, 'BaseUDPTransport'):

    # TODO: remove in 0.4

    import warnings
    warnings.warn(
        "The UDP server has been removed from Sentry since v7.0. "
        "Threaded/async models or a buffer proxy are the preferred replacement.",
        DeprecationWarning,
    )

    # `raven.transport.twisted.TwistedUDPTransport` is broken!
    class TwistedUDPTransport(raven.transport.BaseUDPTransport):

        scheme = ['twisted+udp']

        def __init__(self, parsed_url):
            super(TwistedUDPTransport, self).__init__(parsed_url)
            self.protocol = protocol.DatagramProtocol()
            reactor.listenUDP(0, self.protocol)

        def _send_data(self, data, addr):
            if self.protocol:
                self.protocol.transport.write(data, addr[4])
            else:
                log.err("no active sentry protocol - skip %r", data)

    try:
        raven.Client.register_scheme('twisted+udp', TwistedUDPTransport)
    except raven.transport.DuplicateScheme:
        pass


# ---

settings.add_config(LoggingSettings())
