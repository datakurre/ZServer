# -*- Mode: Python; tab-width: 4 -*-

# monitor client, win32 version
# since we can't do select() on stdin/stdout, we simply
# use threads and blocking sockets.  <sigh>

from __future__ import print_function
from hashlib import md5
import socket
import string
import sys
import six.moves._thread
from six.moves import map
from six.moves import input


def hex_digest(s):
    m = md5()
    m.update(s)
    return string.join(
        [hex(ord(x))[2:] for x in list(m.digest())],
        '',
    )


def reader(lock, sock, password):
    # first grab the timestamp
    ts = sock.recv(1024)[:-2]
    sock.send(hex_digest(ts + password) + '\r\n')
    while 1:
        d = sock.recv(1024)
        if not d:
            lock.release()
            print('Connection closed.  Hit <return> to exit')
            six.moves._thread.exit()
        sys.stdout.write(d)
        sys.stdout.flush()


def writer(lock, sock, barrel="just kidding"):
    while lock.locked():
        sock.send(
            sys.stdin.readline()[:-1] + '\r\n'
        )


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('Usage: %s host port')
        sys.exit(0)
    print('Enter Password: \n')
    p = input()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((sys.argv[1], string.atoi(sys.argv[2])))
    l = six.moves._thread.allocate_lock()
    l.acquire()
    six.moves._thread.start_new_thread(reader, (l, s, p))
    writer(l, s)
