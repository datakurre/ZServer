# -*- coding: utf-8 -*-
import os
import posixpath

from ZPublisher.Iterators import IUnboundStreamIterator
from twisted.internet import threads
from twisted.internet.interfaces import IProducer
from twisted.protocols.basic import FileSender
from twisted.web.resource import IResource
from twisted.web.server import NOT_DONE_YET
from twisted.web.wsgi import _ErrorStream
from twisted.web.wsgi import _InputStream
from twisted.web.wsgi import _wsgiString
from twisted.web.wsgi import _wsgiStringToBytes
from zope.interface import implementer
from ZPublisher.WSGIPublisher import _FILE_TYPES


# from twisted.web.wsgi._WSGIResponse.__init__
def make_environ(request):
    if request.prepath:
        script_name = b'/' + b'/'.join(request.prepath)
    else:
        script_name = b''

    if request.postpath:
        path_info = b'/' + b'/'.join(request.postpath)
    else:
        path_info = b''

    parts = request.uri.split(b'?', 1)
    if len(parts) == 1:
        query_string = b''
    else:
        query_string = parts[1]

    # All keys and values need to be native strings, i.e. of type str in
    # *both* Python 2 and Python 3, so says PEP-3333.
    environ = {
        'REQUEST_METHOD': _wsgiString(request.method),
        'REMOTE_ADDR': _wsgiString(request.getClientAddress().host),
        'SCRIPT_NAME': _wsgiString(script_name),
        'PATH_INFO': _wsgiString(path_info),
        'QUERY_STRING': _wsgiString(query_string),
        'CONTENT_TYPE': _wsgiString(
            request.getHeader(b'content-type') or ''),
        'CONTENT_LENGTH': _wsgiString(
            request.getHeader(b'content-length') or ''),
        'SERVER_NAME': _wsgiString(request.getRequestHostname()),
        'SERVER_PORT': _wsgiString(str(request.getHost().port)),
        'SERVER_PROTOCOL': _wsgiString(request.clientproto)
    }

    # The application object is entirely in control of response headers;
    # disable the default Content-Type value normally provided by
    # twisted.web.server.Request.
    request.defaultContentType = None

    for name, values in request.requestHeaders.getAllRawHeaders():
        name = 'HTTP_' + _wsgiString(name).upper().replace('-', '_')
        # It might be preferable for http.HTTPChannel to clear out
        # newlines.
        environ[name] = ','.join(
            _wsgiString(v) for v in values).replace('\n', ' ')

    environ.update({
        'wsgi.version': (1, 0),
        'wsgi.url_scheme': request.isSecure() and 'https' or 'http',
        'wsgi.run_once': False,
        'wsgi.multithread': True,
        'wsgi.multiprocess': False,
        'wsgi.errors': _ErrorStream(),
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
        'wsgi.input': _InputStream(request.content)
    })

    # Set a flag to indicate this request came through the WebDAV source
    # port server.
    environ['WEBDAV_SOURCE_PORT'] = 1

    if environ['REQUEST_METHOD'] == 'GET':
        path_info = environ['PATH_INFO']
        if os.sep != '/':
            path_info = path_info.replace(os.sep, '/')
        path_info = posixpath.join(path_info, 'manage_DAVget')
        path_info = posixpath.normpath(path_info)
        environ['PATH_INFO'] = path_info

    # Workaround for lousy WebDAV implementation of M$ Office 2K.
    # Requests for "index_html" are *sometimes* send as "index_html."
    # We check the user-agent and remove a trailing dot for PATH_INFO
    # and PATH_TRANSLATED

    if environ.get("HTTP_USER_AGENT", "").find(
            "Microsoft Data Access Internet Publishing Provider") > -1:
        if environ["PATH_INFO"][-1] == '.':
            environ["PATH_INFO"] = environ["PATH_INFO"][:-1]

        if environ["PATH_TRANSLATED"][-1] == '.':
            environ["PATH_TRANSLATED"] = environ["PATH_TRANSLATED"][:-1]

    return environ


class TwistedWebDAVHandler:

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
            lambda *args: self.reactor.callFromThread(
                self.start_response, *args)
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
            if name.lower() not in ('server', 'date'):
                self.request.responseHeaders.addRawHeader(
                    _wsgiStringToBytes(name), _wsgiStringToBytes(value))

        if isinstance(app_iter, _FILE_TYPES) or \
                IUnboundStreamIterator.providedBy(app_iter):
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
            request=request
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
