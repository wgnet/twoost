# coding: utf-8

import zope.interface

from twisted.internet import defer, protocol
from twisted.python import failure
from twisted.application import service

from twoost import timed

import logging
logger = logging.getLogger(__name__)


__all__ = [
    'IHealthChecker',
    'HealthCheckFactory',
]


class IHealthChecker(zope.interface.Interface):

    name = zope.interface.Attribute("service name")

    def checkHealth():
        "Check health of subsystem. Raises exception or return comment string"


# ---

def findApplication(service):
    logger.debug("find application for service %r", service)
    parent = service
    while parent:
        service = parent
        parent = service.IService(service).parent
    return service


def _flatSubservices(root, acc=None):
    if acc is None:
        acc = list()
    acc.append(root)
    sc = service.IServiceCollection(root, None)
    if sc:
        for ss in sc:
            _flatSubservices(ss, acc)
    logger.debug("root %r, subservices %r", root, acc)
    return acc


def _serviceFullName(s):
    nn = []
    s = service.IService(s)
    while s:
        nn.append((s.name or "").strip())
        s = service.IService(s).parent
    nn.append('')
    return "/".join(reversed(nn))


def checkServicesHealth(root_srv, timeout=20):

    logger.debug("check services health, root %r", root_srv)
    ssx = filter(IHealthChecker.providedBy, _flatSubservices(root_srv))

    def on_ok(x, s):
        return True, _serviceFullName(s), str(x or "")

    def trap_ni(f, s):
        f.trap(NotImplementedError)
        logger.debug("service %s not implement health checking", s)

    def on_fail(f, s):
        logger.error("health check failure: %s", f.value)
        return False, _serviceFullName(s), "exc: %s" % f.value

    logger.debug("check health for %d services", len(ssx))
    return defer.gatherResults(
        timed.timeoutDeferred(
            defer.maybeDeferred(IHealthChecker(s).checkHealth)
                .addCallback(on_ok, s)
                .addErrback(trap_ni, s)
                .addErrback(on_fail, s),
            timeout=timeout,
        )
        for s in ssx
    ).addCallback(
        lambda x: list(filter(None, x)),
    )


def formatServicesHealth(ssh):
    mnl = max(len(x[1]) for x in ssh) if ssh else 0
    return "\n".join(
        "{0}\t{1}\t{2}".format(
            'ok' if status else 'fail',
            sname.ljust(mnl + 4),
            msg if len(msg) < 80 else msg[:77] + "..."
        )
        for status, sname, msg
        in sorted(ssh, key=lambda x: x[1])
    )


def parseServicesHealth(ssh):
    res = []
    for line in ssh.splitlines():
        rst, rname, rmsg = line.split("\t", 3)
        res.append((
            rst == 'ok',
            rname.strip(),
            rmsg,
        ))
    return res


class HealthCheckProtocol(protocol.Protocol):

    def responseAppHealth(self, h):
        self.transport.write(formatServicesHealth(h))
        self.transport.loseConnection()

    def connectionMade(self):
        app = self.factory.app
        d = checkServicesHealth(app)
        d.addCallback(self.responseAppHealth)


class HealthCheckFactory(protocol.Factory):

    noisy = False
    protocol = HealthCheckProtocol

    def __init__(self, app):
        self.app = app
