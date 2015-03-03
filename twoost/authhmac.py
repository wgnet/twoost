# utf-8

"""
AuthHMAC is loosely based on the Amazon Web Services authentication scheme v2
but without the Amazon specific components, i.e. it is HMAC for the rest of us.

@see: U(https://github.com/seangeo/auth-hmac)
"""

import time
import hmac
import binascii
import hashlib
import urlparse

import zope.interface

from twisted.internet import defer, reactor
from twisted.cred import credentials, error
from twisted.web.iweb import ICredentialFactory, IAgent
from twisted.web.resource import IResource
from twisted.web.http import stringToDatetime, datetimeToString
from twisted.web.http_headers import Headers

from twoost.web import StringBodyProducer, StringConsumer

try:
    from twisted.web.client import URI
except ImportError:
    from twisted.web.client import _URI as URI


@zope.interface.implementer(IAgent)
class AuthHMACAgent(object):

    """
    An L{Agent} wrapper to handle HTTP authentication.
    Add L(Authentication) header, based on AuthHMAC.
    """
    def __init__(self, agent, accessKey, secretKey):
        self.agent = agent
        self.accessKey = accessKey
        self.secretKey = secretKey
        self._client_server_time_diffs = {}

    @defer.inlineCallbacks
    def request(self, method, uri, headers, bodyProducer):

        if headers is None:
            headers = Headers()
        else:
            headers = headers.copy()

        contentType = headers.getRawHeaders('content-type', [""])[0]
        date = headers.getRawHeaders('date', [""])[0] or self._generateRequestDate(uri)
        headers.setRawHeaders('date', [date])

        uri_origin_form = URI.fromBytes(uri).originForm
        contentMD5 = headers.getRawHeaders('content-md5', [""])[0]

        if not contentMD5 and bodyProducer is not None:

            r = getattr(self.agent, '_reactor') or reactor
            bodyConsumer = StringConsumer(callLater=r.callLater)

            yield bodyProducer.startProducing(bodyConsumer)
            body = bodyConsumer.value()
            bodyProducer = StringBodyProducer(body)

            if body:
                contentMD5 = binascii.b2a_base64(hashlib.md5(body).digest()).strip()
                headers.addRawHeader('content-md5', contentMD5)

        sts = "\n".join([method, contentType or "", contentMD5, date or "", uri_origin_form])
        mac = hmac.new(self.secretKey, sts, digestmod=hashlib.sha1).digest()
        encodedMAC = binascii.b2a_base64(mac).strip()

        auth_header = "AuthHMAC {0}:{1}".format(self.accessKey, encodedMAC)
        headers.addRawHeader('authorization', auth_header)

        d = yield self.agent.request(method, uri, headers, bodyProducer)
        self._handleResponseDate(uri, d)
        defer.returnValue(d)

    def _generateRequestDate(self, uri):
        server = urlparse.urlparse(uri).netloc
        t = time.time() + self._client_server_time_diffs.get(server, 0)
        return datetimeToString(t)

    def _handleResponseDate(self, uri, r):
        d = r.headers.getRawHeaders('date', [None])[0]
        if d:
            server = urlparse.urlparse(uri).netloc
            self._client_server_time_diffs[server] = stringToDatetime(d) - time.time()

    def _respondToChallenge(self, response, method, uri, headers, body):
        authenticate = response.headers.getRawHeaders('www-authenticate')[0]
        scheme, challenge = authenticate.split(None, 1)
        headers = headers.copy()
        headers.addRawHeader(
            'authorization',
            scheme + ' ' + self._responders[scheme].respond(challenge))
        return self._agent.request(method, uri, headers, body)


# --- server

@zope.interface.implementer(
    credentials.ICredentials,
    credentials.IUsernameHashedPassword,
)
class AuthHMACCredentials(object):

    def __init__(
            self, accessKey, mac,
            httpVerb, contentType, contentMD5, date, requestURI,
    ):
        self.accessKey = accessKey
        self.mac = mac
        self._stringToSign = "\n".join(
            [httpVerb, contentType or "", contentMD5, date or "", requestURI]
        )

    @property
    def username(self):
        return self.accessKey

    def checkPassword(self, password):
        return self.checkHMAC(secretKey=password)

    def checkHMAC(self, secretKey):
        h = hmac.new(secretKey, self._stringToSign, digestmod=hashlib.sha1)
        return h.digest() == self.mac


@zope.interface.implementer(ICredentialFactory)
class AuthHMACCredentialFactory(object):

    scheme = 'authhmac'

    # client timestamp included with an authenticated request must be within 15 minutes
    # of the servers system time when the request is received.
    max_request_time_delta = 900

    def __init__(self, realm, max_request_time_delta=None):
        self.realm = realm
        self.max_request_time_delta = max_request_time_delta or self.max_request_time_delta

    def getChallenge(self, request):
        return {'realm': self.realm}

    def checkDate(self, date):
        if not date and self.max_request_time_delta is not None:
            raise error.LoginFailed("Missing mandatory 'date' header")
        t2 = stringToDatetime(date)
        t1 = time.time()
        if self.max_request_time_delta is not None and abs(t1 - t2) > self.max_request_time_delta:
            raise error.LoginFailed("Request 'date' too old")

    def readBodyHash(self, request):
        body = request.content.read()
        if body:
            h = hashlib.md5()
            request.content.seek(0, 0)
            h.update(body)
            return binascii.b2a_base64(h.digest()).strip()

    def decode(self, response, request):

        creds = response.split(":", 1)
        if len(creds) != 2:
            raise error.LoginFailed("Invalid credentials")

        accessKey, encodedMAC = creds
        try:
            mac = binascii.a2b_base64(encodedMAC)
        except binascii.Error:
            raise error.LoginFailed("Invalid credentials")

        contentType = request.getHeader('content-type') or ""

        date = request.getHeader('date') or ""
        self.checkDate(date)

        calculatedContentMD5 = self.readBodyHash(request) or ""
        headerContentMD5 = request.getHeader('content-md5') or ""

        if headerContentMD5 and headerContentMD5 != calculatedContentMD5:
            raise error.LoginFailed("Invalid body md5")

        return AuthHMACCredentials(
            accessKey=accessKey,
            mac=mac,
            httpVerb=request.method,
            contentType=contentType,
            contentMD5=calculatedContentMD5,
            date=date,
            requestURI=request.uri,
        )


# ---

class _SingleResourceRealm(object):

    def __init__(self, resource):
        self.resource = resource

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IResource in interfaces:
            return IResource, self.resource, lambda: None
        raise NotImplementedError


def protectResource(resource, accessKey, secretKey, realm=None):

    from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
    from twisted.web.guard import HTTPAuthSessionWrapper
    from twisted.cred.portal import Portal

    portal_realm = _SingleResourceRealm(resource)

    cred_checker = InMemoryUsernamePasswordDatabaseDontUse()
    cred_checker.addUser(accessKey, secretKey)
    portal = Portal(portal_realm, [cred_checker])

    cred_factory = AuthHMACCredentialFactory(realm or 'twoost-app')
    return HTTPAuthSessionWrapper(portal, [cred_factory])
