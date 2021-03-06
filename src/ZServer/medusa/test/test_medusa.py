# -*- Mode: Python; tab-width: 4 -*-

from __future__ import print_function
import socket
import string
import time
import http_date
from six.moves import range

now = http_date.build_http_date(time.time())


cache_request = string.joinfields(
    ['GET / HTTP/1.0',
     'If-Modified-Since: %s' % now,
     ],
    '\r\n'
) + '\r\n\r\n'

nocache_request = 'GET / HTTP/1.0\r\n\r\n'


def get(request, host='', port=80):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(host, port)
    s.send(request)
    while 1:
        d = s.recv(8192)
        if not d:
            break
    s.close()


class timer(object):

    def __init__(self):
        self.start = time.time()

    def end(self):
        return time.time() - self.start


def test_cache(n=1000):
    t = timer()
    for i in range(n):
        get(cache_request)
    end = t.end()
    print(('cache: %d requests, %.2f seconds, %.2f hits/sec' % (
        n, end, n / end)))


def test_nocache(n=1000):
    t = timer()
    for i in range(n):
        get(nocache_request)
    end = t.end()
    print(('nocache: %d requests, %.2f seconds, %.2f hits/sec' % (
        n, end, n / end)))


if __name__ == '__main__':
    test_cache()
    test_nocache()
