# coding: utf-8

from __future__ import print_function, division, absolute_import

from StringIO import StringIO
from email.mime.text import MIMEText

from twisted.mail.smtp import ESMTPSenderFactory, SMTPSenderFactory
from twisted.internet import reactor, defer

from twoost.conf import settings


import logging
logger = logging.getLogger(__name__)


try:
    from OpenSSL import SSL  # noqa
except ImportError:
    logger.warning("no module OpenSSL.SSL")


def sendMessageFile(
    fromAddress, toAddress,
    messageFile,
    smtpHost=None,
    smtpPort=None,
    smtpUsername=None,
    smtpPassword=None,
):
    """
    @param smtpUsername: The username with which to authenticate.
    @param smtpPassword: The password with which to authenticate.
    @param fromAddress: The SMTP reverse path (ie, MAIL FROM)
    @param toAddress: The SMTP forward path (ie, RCPT TO)
    @param messageFile: A file-like object containing the headers and body of
    the message to send.
    @param smtpHost: The MX host to which to connect.
    @param smtpPort: The port number to which to connect.

    @return: A Deferred which will be called back when the message has been
    sent or which will errback if it cannot be sent.
    """

    resultDeferred = defer.Deferred()

    smtpPort = smtpPort or settings.EMAIL_PORT
    smtpHost = smtpHost or settings.EMAIL_HOST
    smtpUsername = smtpUsername or settings.EMAIL_USER
    smtpPassword = smtpPassword or settings.EMAIL_PASSWORD

    if smtpUsername or smtpPassword:
        senderFactory = ESMTPSenderFactory(
            smtpUsername,
            smtpPassword,
            fromAddress,
            toAddress,
            messageFile,
            resultDeferred,
        )
    else:
        senderFactory = SMTPSenderFactory(
            fromAddress,
            toAddress,
            messageFile,
            resultDeferred,
        )
    reactor.connectTCP(smtpHost, smtpPort, senderFactory)
    return resultDeferred


def send_mail(subject, text, from_email=None, to_addrs=None):

    from_email = from_email or settings.EMAIL_DEFAULT_FROM
    logger.debug("send mail %r from %r to %r", subject, from_email, to_addrs)

    msg = MIMEText(text)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ", ".join(to_addrs)

    return sendMessageFile(
        fromAddress=from_email,
        toAddress=to_addrs,
        messageFile=StringIO(msg.as_string()),
    )
