# -*- Mode: Python; tab-width: 4 -*-
# monitor client, unix version.

from __future__ import print_function
import asyncore
import asynchat
from hashlib import md5
import regsub
import socket
import string
import sys
import os
from six.moves import map
from six.moves import input


class stdin_channel(asyncore.file_dispatcher):
    def handle_read(self):
        data = self.recv(512)
        if not data:
            print('\nclosed.')
            self.sock_channel.close()
            try:
                self.close()
            except:
                pass

        data = regsub.gsub('\n', '\r\n', data)
        self.sock_channel.push(data)

    def writable(self):
        return 0

    def log(self, *ignore):
        pass


class monitor_client(asynchat.async_chat):

    def __init__(self, password, addr=('', 8023), socket_type=socket.AF_INET):
        asynchat.async_chat.__init__(self)
        self.create_socket(socket_type, socket.SOCK_STREAM)
        self.terminator = '\r\n'
        self.connect(addr)
        self.sent_auth = 0
        self.timestamp = ''
        self.password = password

    def collect_incoming_data(self, data):
        if not self.sent_auth:
            self.timestamp = self.timestamp + data
        else:
            sys.stdout.write(data)
            sys.stdout.flush()

    def found_terminator(self):
        if not self.sent_auth:
            self.push(hex_digest(self.timestamp + self.password) + '\r\n')
            self.sent_auth = 1
        else:
            print()

    def handle_close(self):
        # close all the channels, which will make the standard main
        # loop exit.
        list(map(lambda x: x.close(), list(asyncore.socket_map.values())))

    def log(self, *ignore):
        pass


class encrypted_monitor_client(monitor_client):
    "Wrap push() and recv() with a stream cipher"

    def init_cipher(self, cipher, key):
        self.outgoing = cipher.new(key)
        self.incoming = cipher.new(key)

    def push(self, data):
        # push the encrypted data instead
        return monitor_client.push(self, self.outgoing.encrypt(data))

    def recv(self, block_size):
        data = monitor_client.recv(self, block_size)
        if data:
            return self.incoming.decrypt(data)
        else:
            return data


def hex_digest(s):
    m = md5()
    m.update(s)
    return string.join(
        [hex(ord(x))[2:] for x in list(m.digest())],
        '',
    )


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(('Usage: %s host port' % sys.argv[0]))
        sys.exit(0)

    if ('-e' in sys.argv):
        encrypt = 1
        sys.argv.remove('-e')
    else:
        encrypt = 0

    sys.stderr.write('Enter Password: ')
    sys.stderr.flush()
    try:
        os.system('stty -echo')
        p = input()
        print()
    finally:
        os.system('stty echo')
    stdin = stdin_channel(0)
    if len(sys.argv) > 1:
        if encrypt:
            client = encrypted_monitor_client(
                p, (sys.argv[1], string.atoi(sys.argv[2])))
            import sapphire
            client.init_cipher(sapphire, p)
        else:
            client = monitor_client(
                p, (sys.argv[1], string.atoi(sys.argv[2])))
    else:
        # default to local host, 'standard' port
        client = monitor_client(p)
    stdin.sock_channel = client
    asyncore.loop()
