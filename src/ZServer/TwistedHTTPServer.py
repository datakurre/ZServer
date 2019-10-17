# -*- coding: utf-8 -*-
from autobahn.twisted import WebSocketServerFactory
from autobahn.twisted import WebSocketServerProtocol
from autobahn.twisted.resource import WebSocketResource
from copy import deepcopy
from io import BytesIO
from twisted.internet import threads
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import DeferredLock
from twisted.internet.interfaces import IProducer
from twisted.protocols.basic import FileSender
from twisted.web.resource import IResource
from twisted.web.server import NOT_DONE_YET
from twisted.web.server import version
from twisted.web.wsgi import _ErrorStream
from twisted.web.wsgi import _InputStream
from twisted.web.wsgi import _wsgiString
from twisted.web.wsgi import _wsgiStringToBytes
from txzmq import ZmqEndpoint
from txzmq import ZmqEndpointType
from txzmq import ZmqFactory
from txzmq import ZmqPubConnection
from txzmq import ZmqSubConnection
from zope.interface import implementer
from ZPublisher.Iterators import IUnboundStreamIterator
from ZPublisher.WSGIPublisher import _FILE_TYPES
from ZServer import ZOPE_VERSION
from ZServer import ZSERVER_VERSION
from ZServer.HTTPRequest import HTTPRequest
from ZServer.HTTPRequest import WebSocketRequest
import json
import logging
import os
import pickle
import posixpath
import re
import time
import twisted.internet


logger = logging.getLogger("ZServer")


# from twisted.web.wsgi._WSGIResponse.__init__
def make_environ(request):
    if request.prepath:
        script_name = b"/" + b"/".join(request.prepath)
    else:
        script_name = b""

    if request.postpath:
        path_info = b"/" + b"/".join(request.postpath)
    else:
        path_info = b""

    parts = request.uri.split(b"?", 1)
    if len(parts) == 1:
        query_string = b""
    else:
        query_string = parts[1]

    # All keys and values need to be native strings, i.e. of type str in
    # *both* Python 2 and Python 3, so says PEP-3333.
    environ = {
        "REQUEST_METHOD": _wsgiString(request.method),
        "REMOTE_ADDR": _wsgiString(request.getClientAddress().host),
        "SCRIPT_NAME": _wsgiString(script_name),
        "PATH_INFO": _wsgiString(path_info),
        "QUERY_STRING": _wsgiString(query_string),
        "CONTENT_TYPE": _wsgiString(request.getHeader(b"content-type") or ""),
        "CONTENT_LENGTH": _wsgiString(request.getHeader(b"content-length") or ""),
        "HTTPS": request.isSecure() and '1' or False,
        "SERVER_NAME": _wsgiString(request.getRequestHostname()),
        "SERVER_PORT": _wsgiString(str(request.getHost().port)),
        "SERVER_PROTOCOL": _wsgiString(request.clientproto),
        "SERVER_SOFTWARE": "Zope/%s ZServer/%s %s"
        % (ZOPE_VERSION, ZSERVER_VERSION, version.decode("utf-8")),
    }

    # The application object is entirely in control of response headers;
    # disable the default Content-Type value normally provided by
    # twisted.web.server.Request.
    request.defaultContentType = None

    for name, values in request.requestHeaders.getAllRawHeaders():
        name = "HTTP_" + _wsgiString(name).upper().replace("-", "_")
        # It might be preferable for http.HTTPChannel to clear out
        # newlines.
        environ[name] = ",".join(_wsgiString(v) for v in values).replace("\n", " ")

    environ.update(
        {
            "wsgi.version": (1, 0),
            "wsgi.url_scheme": request.isSecure() and "https" or "http",
            "wsgi.run_once": False,
            "wsgi.multithread": True,
            "wsgi.multiprocess": False,
            "wsgi.errors": _ErrorStream(),
            # Attend: request.content was owned by the I/O thread up until
            # this point.  By wrapping it and putting the result into the
            # environment dictionary, it is effectively being given to
            # another thread.  This means that whatever it is, it has to be
            # safe to access it from two different threads.  The access
            # *should* all be serialized (first the I/O thread writes to
            # it, then the WSGI thread reads from it, then the I/O thread
            # closes it).  However, since the request is made available to
            # arbitrary application code during resource traversal, it's
            # possible that some other code might decide to use it in the
            # I/O thread concurrently with its use in the WSGI thread.
            # More likely than not, this will break.  This seems like an
            # unlikely possibility to me, but if it is to be allowed,
            # something here needs to change. -exarkun
            "wsgi.input": _InputStream(request.content),
        }
    )

    return environ


# from twisted.web.wsgi._WSGIResponse.__init__
def make_webdav_environ(request):
    environ = make_environ(request)

    # Set a flag to indicate this request came through the WebDAV source
    # port server.
    environ["WEBDAV_SOURCE_PORT"] = 1

    if environ["REQUEST_METHOD"] == "GET":
        path_info = environ["PATH_INFO"]
        if os.sep != "/":
            path_info = path_info.replace(os.sep, "/")
        path_info = posixpath.join(path_info, "manage_DAVget")
        path_info = posixpath.normpath(path_info)
        environ["PATH_INFO"] = path_info

    # Workaround for lousy WebDAV implementation of M$ Office 2K.
    # Requests for "index_html" are *sometimes* send as "index_html."
    # We check the user-agent and remove a trailing dot for PATH_INFO
    # and PATH_TRANSLATED

    if (
        environ.get("HTTP_USER_AGENT", "").find(
            "Microsoft Data Access Internet Publishing Provider"
        )
        > -1
    ):
        if environ["PATH_INFO"][-1] == ".":
            environ["PATH_INFO"] = environ["PATH_INFO"][:-1]

        if environ["PATH_TRANSLATED"][-1] == ".":
            environ["PATH_TRANSLATED"] = environ["PATH_TRANSLATED"][:-1]

    return environ


class TwistedHTTPHandler:

    _producer = None
    _requestFinished = False

    def __init__(self, reactor, threadpool, publish_module, request):
        self.reactor = reactor
        self.threadpool = threadpool
        self.publish_module = publish_module
        self.request = request
        self.environ = make_environ(request)
        self.status = None
        self.headers = None

        self.request.setHeader(
            "server", self.environ["SERVER_SOFTWARE"].encode("utf-8")
        )
        self.request.notifyFinish().addBoth(self._finished)

    def _finished(self, ignored):
        """
        Record the end of the response generation for the request being
        serviced.
        """
        self._requestFinished = True
        if IProducer.providedBy(self._producer):
            if self._producer.file is not None:
                self._producer.file.close()
            self._producer.stopProducing()

    def start(self):
        d = threads.deferToThread(
            self.publish_module,
            self.environ,
            lambda *args: self.reactor.callFromThread(self.start_response, *args),
            _request_factory=HTTPRequest,
        )
        d.addCallback(self.finish_response)

    def start_response(self, status, headers, excInfo=None):
        self.status = status
        self.headers = headers

    def finish_response(self, app_iter):
        code, message = self.status.split(None, 1)
        code = int(code)
        self.request.setResponseCode(code, _wsgiStringToBytes(message))

        for name, value in self.headers:
            # Don't allow the application to control these required headers.
            if name.lower() not in ("server", "date"):
                self.request.responseHeaders.addRawHeader(
                    _wsgiStringToBytes(name), _wsgiStringToBytes(value)
                )

        if isinstance(app_iter, _FILE_TYPES) or IUnboundStreamIterator.providedBy(
            app_iter
        ):
            if not self._requestFinished:
                self._producer = FileSender()
                d = self._producer.beginFileTransfer(app_iter, self.request)
                d.addBoth(lambda *args: self.stop())
        else:
            for elem in app_iter:
                if not self._requestFinished:
                    self.request.write(elem)
            self.stop()

    def stop(self):
        if IProducer.providedBy(self._producer):
            if self._producer.file is not None:
                self._producer.file.close()
            self._producer = None
        if not self._requestFinished:
            self.request.finish()


def get_physical_path_from_vhm_path(path):
    """Return the most probable physical from possible VHM-path."""
    # Find physical path from /VirtualHostBase/http/domain:80/site/VirtualHostRoot
    if path.startswith("/VirtualHostBase/"):
        # Get everything after /VirtualHostBase/
        path = path[len("/VirtualHostBase/") :]
        # Get everything before /VirtualHostRoot
        if "/VirtualHostRoot" in path:
            path = path.split("/VirtualHostRoot")[0]
        # Drop http/domain:80
        path = "/" + path.split("/", 2)[-1]
    return path


# Only one guard call can be made at time to not consume more that
# one thread from the ZServer worker pool at the same time
WS_GUARD_CALL_MUTEX = DeferredLock()


class PubSubServerProtocol(WebSocketServerProtocol):

    _filters = []
    _guards = {}

    _zmq = None

    _environ = None
    _publish_module = None
    _threadpool = None
    _reactor = None

    def onConnect(self, request):
        global pub_sock
        if pub_sock is not None:
            zf = ZmqFactory()
            e = ZmqEndpoint(ZmqEndpointType.connect, pub_sock)
            s = ZmqSubConnection(zf, e)
            s.gotMessage = lambda payload, topic: self.gotMessage(payload, topic)
            self._zmq = s
            self._guards = {}
            self._filters = []

            # By default, subscribe messages from the current path and below
            default_filter = get_physical_path_from_vhm_path(self._environ["PATH_INFO"])
            logger.debug("SUBSCRIBE " + default_filter)
            self._zmq.subscribe(default_filter.encode('utf-8'))
            self._filters.append(default_filter)

        # Cleanup inherited environ
        self._environ["HTTP_CONNECTION"] = "keep-alive"
        for env in [
            "HTTP_UPGRADE",
            "HTTP_SEC_WEBSOCKET_EXTENSIONS",
            "HTTP_SEC_WEBSOCKET_KEY",
            "HTTP_SEC_WEBSOCKET_VERSION",
            "wsgi.errors",
            "wsgi.input",
        ]:
            if env in self._environ:
                del self._environ[env]

    # noinspection PyPep8Naming
    def gotMessage(self, payload, topic):  # noqa
        message = pickle.loads(payload)

        now = time.time()
        deferred_guards = []
        for guard, settings in (message.get("guards") or {}).items():
            if guard in self._guards and now < self._guards[guard]["expires"]:
                if set(settings["tokens"]) & set(self._guards[guard]["tokens"]):
                    continue  # OK and continue to check the next guard
                else:
                    return  # Forbidden (according to cached guard)

            environ = deepcopy(self._environ)
            environ["REQUEST_METHOD"] = "GET"
            environ["PATH_INFO"] = settings["method"]
            environ["wsgi.errors"] = _ErrorStream()
            environ["wsgi.input"] = _InputStream(BytesIO(b""))

            # DeferredLock should is used to limit to only ever use single working
            # thread for guard checks
            call = Deferred()
            WS_GUARD_CALL_MUTEX.acquire().addCallback(
                lambda _, d=call: threads.deferToThread(
                    self._publish_module,
                    environ,
                    lambda *args: None,
                    _request_factory=WebSocketRequest,
                ).addBoth(lambda _: [WS_GUARD_CALL_MUTEX.release(), d.callback(_)])
            )
            deferred_guards.append(call)

        if deferred_guards:
            dl = DeferredList(deferred_guards)
            dl.addCallback(lambda result: self.gotMessageFinished(message, result))
        else:
            super(PubSubServerProtocol, self).sendMessage(
                message["payload"], isBinary=False
            )

    def gotMessageFinished(self, message, result):
        for (success, app_iter) in result:
            if not success or not app_iter:
                # Could not check guard; message dropped
                return

            # noinspection PyBroadException
            for guard, settings in json.loads(b"".join(app_iter)).items():
                self._guards[guard] = {
                    "tokens": settings["tokens"],
                    "expires": settings.get("expires") or 0,
                }

        now = time.time()
        for guard, settings in (message.get("guards") or {}).items():
            if (
                guard not in self._guards
                or now > self._guards[guard]["expires"]
                or not (set(settings["tokens"]) & set(self._guards[guard]["tokens"]))
            ):
                return  # Forbidden (according to guard)

        super(PubSubServerProtocol, self).sendMessage(
            message["payload"], isBinary=False
        )

    def onMessage(self, message, isBinary):
        # noinspection PyBroadException
        try:
            data = json.loads(message)
        except Exception as e:
            super(PubSubServerProtocol, self).sendMessage(
                json.dumps({"statusCode": 500, "reasonPhrase": str(e)}).encode("utf-8"), isBinary=False
            )
            return

        method = data.get('method') or ''
        path = data.get('path') or ''
        if self._zmq and method == 'SUBSCRIBE' and path.startswith('/'):
            if data.get('path') in self._filters or len(self._filters) >= 1000:
                super(PubSubServerProtocol, self).sendMessage(
                    json.dumps({"statusCode": 304}).encode("utf-8"), isBinary=False
                )
            else:
                logger.debug("SUBSCRIBE " + path)
                self._zmq.subscribe(path.encode('utf-8'))
                self._filters.append(path)
                super(PubSubServerProtocol, self).sendMessage(
                    json.dumps({"statusCode": 200}).encode("utf-8"), isBinary=False
                )

        elif self._zmq and method == 'UNSUBSCRIBE' and path.startswith('/'):
            if path not in self._filters:
                super(PubSubServerProtocol, self).sendMessage(
                    json.dumps({"status_code": 304}).encode("utf-8"), isBinary=False
                )
            else:
                logger.debug("UNSUBSCRIBE " + path)
                self._zmq.unsubscribe(path.encode('utf-8'))
                self._filters.remove(path)
                super(PubSubServerProtocol, self).sendMessage(
                    json.dumps({"status_code": 200}).encode("utf-8"), isBinary=False
                )
        else:
            # TODO: Pass at least methods GET, POST, PUT and DELETE to publish_module
            super(PubSubServerProtocol, self).sendMessage(
                json.dumps({"statusCode": 304, "reasonPhrase": "No Operation"}).encode("utf-8"), isBinary=False
            )

    def _connectionLost(self, reason):
        if self._zmq is not None:
            for prefix_filter in self._filters:
                self._zmq.unsubscribe(prefix_filter.encode('utf-8'))
        super(PubSubServerProtocol, self)._connectionLost(reason)


pub = None
pub_sock = None


def publish(message, topic):
    global pub

    # Assert message format
    assert isinstance(message, dict)
    assert isinstance(message["guards"], dict)
    for guard, settings in message["guards"].items():
        assert isinstance(guard, str)
        assert isinstance(settings["method"], str)
        assert isinstance(settings["tokens"], list) or isinstance(
            settings["tokens"], set
        )
        for token in settings["tokens"]:
            assert isinstance(token, str)
    assert isinstance(message["payload"], bytes)

    # Pickle payload
    payload = pickle.dumps(message)

    # Publish
    if not isinstance(topic, bytes):
        logger.debug("PUBLISH " + topic)
        pub.publish(payload, (topic or "").encode("utf-8"))
    else:
        logger.debug("PUBLISH " + topic.decode("utf-8"))
        pub.publish(payload, topic)


class CustomWebSocketResource(WebSocketResource):
    _request = None
    _reactor = None
    _threadpool = None
    _publish_module = None

    def __init__(self, factory):
        super(CustomWebSocketResource, self).__init__(factory)
        self._factory = self
        self.__factory = factory

    def buildProtocol(self, *args, **kwargs):
        protocol = self.__factory.buildProtocol(*args, **kwargs)
        protocol._environ = make_environ(self._request)
        protocol._reactor = self._reactor
        protocol._threadpool = self._threadpool
        protocol._publish_module = self._publish_module
        return protocol

    def render(self, request, reactor=None, threadpool=None, publish_module=None):
        try:
            self._request = request
            self._reactor = reactor
            self._threadpool = threadpool
            self._publish_module = publish_module
            response = super(CustomWebSocketResource, self).render(request)
            return response
        finally:
            self._request = None
            self._reactor = None
            self._threadpool = None
            self._publish_module = None


@implementer(IResource)
class TwistedHTTPServer:
    """
    An IResource implementation which delegates responsibility for all
    resources hierarchically inferior to it to ZPublisher.

    _reactor: An IReactorThreads provider which will be passed on to
              HTTPResponse to schedule calls in the I/O thread.

    _threadpool: A ThreadPool which will be passed on to
                 HTTPResponse to run ZPublisher.

    _publish_module: ZPublisher publish_module method.
    """

    # Further resource segments are left up to ZPublisher object to
    # handle.
    isLeaf = True

    def __init__(self, reactor, threadpool, publish_module, websocket_ipc=None):
        self._reactor = reactor
        self._threadpool = threadpool
        self._publish_module = publish_module

        # ZMQ pubsub
        if websocket_ipc:
            path = ("ipc://" + os.path.abspath(websocket_ipc)).encode("utf-8")
            zf = ZmqFactory()
            e = ZmqEndpoint(ZmqEndpointType.bind, os.path.join(path))
            s = ZmqPubConnection(zf, e)
            global pub
            global pub_sock
            pub = s
            pub_sock = path

        # WebSockets
        factory = WebSocketServerFactory()
        factory.protocol = PubSubServerProtocol
        self.ws = CustomWebSocketResource(factory)

    def render(self, request):
        if request.getHeader("upgrade") == "websocket":
            return self.ws.render(
                request=request,
                reactor=self._reactor,
                threadpool=self._threadpool,
                publish_module=self._publish_module,
            )
        else:
            handler = TwistedHTTPHandler(
                reactor=self._reactor,
                threadpool=self._threadpool,
                publish_module=self._publish_module,
                request=request,
            )
            handler.start()
            return NOT_DONE_YET

    def getChildWithDefault(self, name, request):
        """
        Reject attempts to retrieve a child resource. All path segments beyond
        the one which refers to this resource are handled by ZPublisher.
        """
        raise RuntimeError("Cannot get IResource children from HTTPServer")

    def putChild(self, path, child):
        """
        Reject attempts to add a child resource to this resource. ZPublisher
        handles all path segments beneath this resource, so IResource children
        can never be found.
        """
        raise RuntimeError("Cannot put IResource children under HTTPServer")


class TwistedWebDAVHandler:

    _producer = None
    _requestFinished = False

    def __init__(self, reactor, threadpool, publish_module, request):
        self.reactor = reactor
        self.threadpool = threadpool
        self.publish_module = publish_module
        self.request = request
        self.environ = make_webdav_environ(request)
        self.status = None
        self.headers = None

        self.request.setHeader(
            "server", self.environ["SERVER_SOFTWARE"].encode("utf-8")
        )
        self.request.notifyFinish().addBoth(self._finished)

    def _finished(self, ignored):
        """
        Record the end of the response generation for the request being
        serviced.
        """
        self._requestFinished = True
        if IProducer.providedBy(self._producer):
            if self._producer.file is not None:
                self._producer.file.close()
            self._producer.stopProducing()

    def start(self):
        d = threads.deferToThread(
            self.publish_module,
            self.environ,
            lambda *args: self.reactor.callFromThread(self.start_response, *args),
            _request_factory=HTTPRequest,
        )
        d.addCallback(self.finish_response)

    def start_response(self, status, headers, excInfo=None):
        self.status = status
        self.headers = headers

    def finish_response(self, app_iter):
        code, message = self.status.split(None, 1)
        code = int(code)
        self.request.setResponseCode(code, _wsgiStringToBytes(message))

        for name, value in self.headers:
            # Don't allow the application to control these required headers.
            if name.lower() not in ("server", "date"):
                self.request.responseHeaders.addRawHeader(
                    _wsgiStringToBytes(name), _wsgiStringToBytes(value)
                )

        if isinstance(app_iter, _FILE_TYPES) or IUnboundStreamIterator.providedBy(
            app_iter
        ):
            if not self._requestFinished:
                self._producer = FileSender()
                d = self._producer.beginFileTransfer(app_iter, self.request)
                d.addBoth(lambda *args: self.stop())
        else:
            for elem in app_iter:
                if not self._requestFinished:
                    self.request.write(elem)
            self.stop()

    def stop(self):
        if IProducer.providedBy(self._producer):
            if self._producer.file is not None:
                self._producer.file.close()
            self._producer = None
        if not self._requestFinished:
            self.request.finish()


@implementer(IResource)
class TwistedWebDAVServer:
    """
    An IResource implementation which delegates responsibility for all
    resources hierarchically inferior to it to ZPublisher.

    _reactor: An IReactorThreads provider which will be passed on to
              WebDAVResponse to schedule calls in the I/O thread.

    _threadpool: A ThreadPool which will be passed on to
                 WebDAVResponse to run ZPublisher.

    _publish_module: ZPublisher publish_module method.
    """

    # Further resource segments are left up to ZPublisher object to
    # handle.
    isLeaf = True

    def __init__(self, reactor, threadpool, publish_module):
        self._reactor = reactor
        self._threadpool = threadpool
        self._publish_module = publish_module

    def render(self, request):
        handler = TwistedWebDAVHandler(
            reactor=self._reactor,
            threadpool=self._threadpool,
            publish_module=self._publish_module,
            request=request,
        )
        handler.start()
        return NOT_DONE_YET

    def getChildWithDefault(self, name, request):
        """
        Reject attempts to retrieve a child resource. All path segments beyond
        the one which refers to this resource are handled by ZPublisher.
        """
        raise RuntimeError("Cannot get IResource children from WebDAVServer")

    def putChild(self, path, child):
        """
        Reject attempts to add a child resource to this resource. ZPublisher
        handles all path segments beneath this resource, so IResource children
        can never be found.
        """
        raise RuntimeError("Cannot put IResource children under WebDAVServer")
