# coding: utf-8

import zope.interface

from twisted.internet import defer, protocol
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
        "Check health of subsystem. Returns tuple `(bool, str)`"


# ---

def findApplication(service):
    parent = service
    while parent:
        service = parent
        parent = service.IService(service).parent
    return service


def _flatSubservices(root, acc=None):
    if acc is None:
        acc = list()
    acc.append(root)
    if service.IServiceCollection.providedBy(root):
        _flatSubservices(service.IServiceCollection(root), acc)
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

    ssx = filter(IHealthChecker.providedBy, _flatSubservices(root_srv))

    def on_ok(x, s):
        status, msg = x
        return status, _serviceFullName(s), msg

    def on_fail(f, s):
        logger.error("health check failure: %s", f.value)
        return False, _serviceFullName(s), "exc: %s" % f.value

    return defer.gatherResults(
        timed.timeoutDeferred(
            defer.maybeDeferred(IHealthChecker(s).checkHealth)
                .addCallback(on_ok, s)
                .addErrback(on_fail, s),
            timeout=timeout,
        )
        for s in ssx
    )


def formatServicesHealth(ssh):
    mnl = max(x[1] for x in ssh)
    return "\n".join(
        "{0}\t{1}\t{2}".format(
            'ok' if status else 'fail',
            sname.ljust(mnl),
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
        s = self.factory.service
        d = s.checkApplicationHealth()
        d.addCallback(self.responseAppHealth)


class HealthCheckFactory(protocol.Factory):

    noisy = False
    protocol = HealthCheckProtocol

    def __init__(self, app):
        self.app = app
