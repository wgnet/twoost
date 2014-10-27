# coding: utf-8

import os
from twisted.manhole.telnet import ShellFactory, Shell

import logging
logger = logging.getLogger(__name__)


class AnonymousShell(Shell):

    mode = 'Command'

    def loginPrompt(self):
        # no login
        return ">>> "

    def welcomeMessage(self):
        processid = os.getpid()
        workerid = os.environ.get('WORKER_ID') or "-"
        return "twoost: worker %s, pid %s\r\n" % (workerid, processid)

    def doCommand(self, cmd):
        logger.info("execute cmd: %r", cmd)
        return Shell.doCommand(self, cmd)


class AnonymousShellFactory(ShellFactory):

    protocol = AnonymousShell

    def __init__(self, namespace=None):
        ShellFactory.__init__(self)
        self.namespace.update(namespace or {})
