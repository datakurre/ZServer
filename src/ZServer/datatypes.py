##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""ZConfig datatype support for ZServer.

Each server type is represented by a ServerFactory instance.
"""
from __future__ import absolute_import

import socket

import ZConfig
import twisted.internet
from ZPublisher.WSGIPublisher import publish_module
from ZServer.AccessLogger import AccessLogger
from ZServer.TwistedHTTPServer import TwistedHTTPServer
from ZServer.TwistedHTTPServer import TwistedWebDAVServer
from twisted.application.service import Service
from twisted.internet.endpoints import serverFromString
from twisted.python.runtime import seconds
from twisted.web.http import datetimeToLogString
from twisted.web.server import Site
from twisted.web.wsgi import WSGIResource


class ServerFactory(object):
    def __init__(self, address=None):
        self.ip = None
        if address is None:
            self.host = None
            self.port = None
        else:
            self.host, self.port = address

    def prepare(self, defaulthost='', dnsresolver=None,
                module=None, env=None, portbase=None):
        if not self.host:
            ip = socket.gethostbyname(defaulthost)
            self._set_default_host(defaulthost, ip)
        else:
            address_info = socket.getaddrinfo(self.host, self.port)
            ips = [info[4][0] for info in address_info]
            self.ip = ips[0]
        self.dnsresolver = dnsresolver
        self.module = module
        self.cgienv = env
        if portbase and self.port is not None:
            self.port += portbase

    def _set_default_host(self, host, ip):
        self.host = host
        self.ip = ip

    def servertype(self):
        s = self.__class__.__name__
        if s.endswith("Factory"):
            s = s[:-7]
        return s

    def create(self):
        raise NotImplementedError(
            "Concrete ServerFactory classes must implement create().")


class ZServerSite(Site):
    def __init__(self, *args, **kwargs):
        Site.__init__(self, *args, **kwargs)
        self.logger = AccessLogger()

    def _updateLogDateTime(self):
        """
        Update log datetime periodically, so we aren't always recalculating it.
        """
        self._logDateTime = datetimeToLogString(seconds())
        self._logDateTimeCall = self._reactor.callLater(1, self._updateLogDateTime)

    def log(self, request):
        line = self._logFormatter(self._logDateTime, request) + u"\n"
        self.logger.log(line)


class TwistedHTTPServerFactory(Service, ServerFactory):

    endpoint = None
    site = None

    def __init__(self, section):
        if not section.address:
            raise ZConfig.ConfigurationError(
                "No 'address' settings found "
                "within the 'http-server' or 'webdav-source-server' section")
        ServerFactory.__init__(self, section.address)
        self.force_connection_close = section.force_connection_close
        # webdav-source-server sections won't have webdav_source_clients:
        webdav_clients = getattr(section, "webdav_source_clients", None)
        self.fast_listen = getattr(section, 'fast_listen', True)
        self.webdav_source_clients = webdav_clients
        self.use_wsgi = section.use_wsgi
        self.websocket_ipc = section.websocket_ipc

    def create(self):
        if self.use_wsgi:
            resource = WSGIResource(
                reactor=twisted.internet.reactor,
                threadpool=twisted.internet.reactor.getThreadPool(),
                application=publish_module,
            )
        else:
            resource = TwistedHTTPServer(
                reactor=twisted.internet.reactor,
                threadpool=twisted.internet.reactor.getThreadPool(),
                publish_module=publish_module,
                websocket_ipc=self.websocket_ipc,
            )
        self.site = ZServerSite(resource)
        self.endpoint = serverFromString(
            twisted.internet.reactor,
            'tcp:port={port}'.format(port=self.port),
        )
        if self.fast_listen:
            self.listen()
        return self

    def listen(self, *args):
        self.endpoint.listen(self.site)


class TwistedWebDAVSourceServerFactory(Service, ServerFactory):
    def __init__(self, section):
        if not section.address:
            raise ZConfig.ConfigurationError(
                "No 'address' settings found "
                "within the 'http-server' or 'webdav-source-server' section")
        ServerFactory.__init__(self, section.address)
        self.force_connection_close = section.force_connection_close
        # webdav-source-server sections won't have webdav_source_clients:
        webdav_clients = getattr(section, "webdav_source_clients", None)
        self.fast_listen = getattr(section, 'fast_listen', True)
        self.webdav_source_clients = webdav_clients
        self.use_wsgi = section.use_wsgi

    def create(self):
        if self.use_wsgi:
            resource = WSGIResource(
                reactor=twisted.internet.reactor,
                threadpool=twisted.internet.reactor.getThreadPool(),
                application=publish_module,
            )
        else:
            resource = TwistedWebDAVServer(
                reactor=twisted.internet.reactor,
                threadpool=twisted.internet.reactor.getThreadPool(),
                publish_module=publish_module,
            )
        site = ZServerSite(resource)
        endpoint = serverFromString(
            twisted.internet.reactor,
            'tcp:port={port}'.format(port=self.port),
        )
        endpoint.listen(site)
        return self


class HTTPServerFactory(ServerFactory):

    def __init__(self, section):
        from ZServer import HTTPServer
        if not section.address:
            raise ZConfig.ConfigurationError(
                "No 'address' settings found "
                "within the 'http-server' or 'webdav-source-server' section")
        ServerFactory.__init__(self, section.address)
        self.server_class = HTTPServer.zhttp_server
        self.force_connection_close = section.force_connection_close
        # webdav-source-server sections won't have webdav_source_clients:
        webdav_clients = getattr(section, "webdav_source_clients", None)
        self.fast_listen = getattr(section, 'fast_listen', True)
        self.webdav_source_clients = webdav_clients
        self.use_wsgi = section.use_wsgi

    def create(self):
        from ZServer.AccessLogger import access_logger
        handler = self.createHandler()
        handler._force_connection_close = self.force_connection_close
        if self.webdav_source_clients:
            handler.set_webdav_source_clients(self.webdav_source_clients)
        server = self.server_class(ip=self.ip, port=self.port,
                                   resolver=self.dnsresolver,
                                   fast_listen=self.fast_listen,
                                   logger_object=access_logger)
        server.install_handler(handler)
        return server

    def createHandler(self):
        from ZServer import HTTPServer
        if self.use_wsgi:
            return HTTPServer.zwsgi_handler(self.module, '', self.cgienv)
        else:
            return HTTPServer.zhttp_handler(self.module, '', self.cgienv)


class WebDAVSourceServerFactory(HTTPServerFactory):

    def __init__(self, section):
        from ZServer import HTTPServer
        HTTPServerFactory.__init__(self, section)
        self.server_class = HTTPServer.zwebdav_server

    def createHandler(self):
        from ZServer.WebDAVSrcHandler import WebDAVSrcHandler
        return WebDAVSrcHandler(self.module, '', self.cgienv)


class FTPServerFactory(ServerFactory):
    def __init__(self, section):
        if not section.address:
            raise ZConfig.ConfigurationError(
                "No 'address' settings found within the 'ftp-server' section")
        ServerFactory.__init__(self, section.address)

    def create(self):
        from ZServer.AccessLogger import access_logger
        from ZServer.FTPServer import FTPServer
        return FTPServer(ip=self.ip, hostname=self.host, port=self.port,
                         module=self.module, resolver=self.dnsresolver,
                         logger_object=access_logger)


class PCGIServerFactory(ServerFactory):
    def __init__(self, section):
        ServerFactory.__init__(self)
        self.path = section.path

    def create(self):
        from ZServer.AccessLogger import access_logger
        from ZServer.PCGIServer import PCGIServer
        return PCGIServer(ip=self.ip, port=self.port,
                          module=self.module, resolver=self.dnsresolver,
                          pcgi_file=self.path,
                          logger_object=access_logger)


class FCGIServerFactory(ServerFactory):
    def __init__(self, section):

        import warnings
        warnings.warn("Using FastCGI is deprecated. You should use mod_proxy "
                      "to run Zope with Apache", DeprecationWarning,
                      stacklevel=2)

        import socket
        if section.address.family == socket.AF_INET:
            address = section.address.address
            path = None
        else:
            address = None
            path = section.address.address
        ServerFactory.__init__(self, address)
        self.path = path

    def _set_default_host(self, host, ip):
        if self.path is None:
            ServerFactory._set_default_host(self, host, ip)

    def create(self):
        from ZServer.AccessLogger import access_logger
        from ZServer.FCGIServer import FCGIServer
        return FCGIServer(ip=self.ip, port=self.port,
                          socket_file=self.path,
                          module=self.module, resolver=self.dnsresolver,
                          logger_object=access_logger)


class MonitorServerFactory(ServerFactory):
    def __init__(self, section):
        ServerFactory.__init__(self, section.address)

    def create(self):
        password = self.getPassword()
        if password is None:
            msg = ('Monitor server not started because no emergency user '
                   'exists.')
            import logging
            LOG = logging.getLogger('Zope')
            LOG.error(msg)
            return
        from ZServer.medusa.monitor import secure_monitor_server
        return secure_monitor_server(hostname=self.host, port=self.port,
                                     password=password)

    def getPassword(self):
        # XXX This is really out of place; there should be a better
        # way.  For now, at least we can make it a separate method.

        from AccessControl.User import emergency_user
        if hasattr(emergency_user, '__null_user__'):
            pw = None
        else:
            pw = emergency_user._getPassword()
        return pw


class ICPServerFactory(ServerFactory):
    def __init__(self, section):
        ServerFactory.__init__(self, section.address)

    def create(self):
        from ZServer.ICPServer import ICPServer
        return ICPServer(self.ip, self.port)


class ClockServerFactory(ServerFactory):
    def __init__(self, section):
        ServerFactory.__init__(self)
        self.method = section.method
        self.period = section.period
        self.user = section.user
        self.password = section.password
        self.hostheader = section.host
        self.host = None  # appease configuration machinery

    def create(self):
        from ZServer.ClockServer import ClockServer
        from ZServer.AccessLogger import access_logger
        return ClockServer(self.method, self.period, self.user,
                           self.password, self.hostheader, access_logger)
