#!/usr/bin/env python
# encoding: utf-8
import time
import socket

import functools
import collections

import errno
from typing import Union, Dict, Optional

from tornado import gen
from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from new_stack import NullContext, wrap
import tornado.gen

_UTF8_TYPES = (bytes, type(None))

def utf8(value: Union[None, str, bytes]) -> Optional[bytes]:  # noqa: F811
    """Converts a string argument to a byte string.

    If the argument is already a byte string or None, it is returned unchanged.
    Otherwise it must be a unicode string and is encoded as utf8.
    """
    if isinstance(value, _UTF8_TYPES):
        return value
    if not isinstance(value, unicode_type):
        raise TypeError("Expected bytes, unicode, or None; got %r" % type(value))
    return value.encode("utf-8")


class UDPRequest(object):
    def __init__(self, url, method = None, body = None, proxy_host=None, proxy_port=None):
        self.url = url
        self.method = method
        self.body = body
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        import urllib
        type, uri = urllib.parse.splittype(self.url)
        self._proxy, self.__handler = urllib.parse.splithost(uri)
        self.proxy_host, self.proxy_port = self._proxy.split(':')

    @property
    def headers(self):
        pass

    @property
    def headers(self):
        return self._headers  # type: ignore

    @headers.setter
    def headers(self, value: Union[Dict[str, str],]) -> None:
        if value is None:
            self._headers = None
        else:
            self._headers = value  # type: ignore

    @property
    def body(self) -> bytes:
        return self._body

    @body.setter
    def body(self, value: Union[bytes, str]) -> None:
        self._body = utf8(value)


class _UDPConnection(object):
    def __init__(self, client, request,
                 release_callback,
                 final_callback,
                 max_buffer_size):

        self.start_time = time.time()
        self.client = client
        self.request = request
        self.release_callback = release_callback
        self.final_callback = final_callback
        addrinfo = socket.getaddrinfo(request.proxy_host, request.proxy_port,
                                      socket.AF_INET,
                                      socket.SOCK_DGRAM,
                                      0, 0
                                      )
        af, socktype, proto, ai_canonname, sockaddr = addrinfo[0]

        self.stream = IOStream(socket.socket(af, socktype, proto),
                               max_buffer_size=2500)

        self.stream.connect(sockaddr).add_done_callback(self._on_connect)


    def _on_connect(self, future):
        print(future)
        if future.done():
            self.stream.write(b"\r\n").add_done_callback(self._on_response)


    def _on_response(self, future):
        if self.release_callback is not None:
            release_callback = self.release_callback
            self.release_callback = None
            release_callback()
        # if self.final_callback:
        #     self.final_callback()
        self.stream.close()


class AsyncUDPClient(object):
    def __init__(self, io_loop=None):
        self.io_loop = io_loop or IOLoop.instance()
        self.max_clients = 10
        self.queue = collections.deque()
        self.active = {}
        self.max_buffer_size = 2500


    def fetch(self, url, callback=None, **kwargs):
        request = UDPRequest(url)
        self.queue.append((request, 'callback'))
        self._process_queue()


    def _process_queue(self):
        with NullContext():
            while self.queue and len(self.active) < self.max_clients:
                request, callback = self.queue.popleft()
                key = object()
                self.active[key] = (request, callback)
                _UDPConnection(self, request,
                               functools.partial(self._release_fetch, key),
                               callback,
                               self.max_buffer_size)

    def _release_fetch(self, key):
        del self.active[key]
        self._process_queue()


def print_hello():
   return '2222'


max_client = 10000

i = 0

clients = [AsyncUDPClient() for client in range(max_client)]

while i < max_client:
   clients[i].fetch("udp://10.1.4.6:8888")
   i += 1

IOLoop.instance().start()


