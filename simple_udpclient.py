#!/usr/bin/env python
# encoding: utf-8

from tornado import gen
from tornado import httputil
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.netutil import (
    Resolver,
    OverrideResolver,
    is_valid_ip,
)
from tornado.log import gen_log

import collections
import functools
import re
import socket
import sys
import time
import urllib.parse

from typing import Dict, Any, Callable, Optional, Type
from types import TracebackType
import typing

from udpclient import UDPClient

if typing.TYPE_CHECKING:
    from typing import Deque, Tuple, List  # noqa: F401


from asynclient import (
    AsyncUDPClient, 
    UDPRequest, 
    UDPResponse, 
    UDPClientError,
)


class UDPTimeoutError(UDPClientError):
    """Error raised by SimpleAsyncHTTPClient on timeout.

    For historical reasons, this is a subclass of `.HTTPClientError`
    which simulates a response code of 599.

    .. versionadded:: 5.1
    """

    def __init__(self, message: str) -> None:
        super(UDPTimeoutError, self).__init__(599, message=message)

    def __str__(self) -> str:
        return self.message or "Timeout"


class UDPStreamClosedError(UDPClientError):

    def __init__(self, message: str) -> None:
        super(UDPStreamClosedError, self).__init__(599, message=message)

    def __str__(self) -> str:
        return self.message or "Stream closed"


class SimpleAsyncUDPClient(AsyncUDPClient):
    def initialize(  # type: ignore
            self,
            max_clients: int = 10,
            hostname_mapping: Dict[str, str] = None,
            max_buffer_size: int = 104857600,
            resolver: Resolver = None,
            defaults: Dict[str, Any] = None,
            max_header_size: int = None,
            max_body_size: int = None,
    ) -> None:

        super(SimpleAsyncUDPClient, self).initialize(defaults=defaults)
        self.max_clients = max_clients
        self.queue = (
            collections.deque()
        )  # type: Deque[Tuple[object, UDPRequest, Callable[[UDPResponse], None]]]
        self.active = (
            {}
        )  # type: Dict[object, Tuple[UDPRequest, Callable[[UDPResponse], None]]]
        self.waiting = (
            {}
        )  # type: Dict[object, Tuple[UDPRequest, Callable[[UDPResponse], None], object]]
        self.max_buffer_size = max_buffer_size
        self.max_header_size = max_header_size
        self.max_body_size = max_body_size
        # TCPClient could create a Resolver for us, but we have to do it
        # ourselves to support hostname_mapping.

        if resolver:
            self.resolver = resolver

            self.own_resolver = False
        else:
            self.resolver = Resolver()
            self.own_resolver = True
        if hostname_mapping is not None:
            self.resolver = OverrideResolver(
                resolver=self.resolver, mapping=hostname_mapping
            )
        self.tcp_client = UDPClient(resolver=self.resolver)

    def close(self) -> None:
        super(SimpleAsyncUDPClient, self).close()
        if self.own_resolver:
            self.resolver.close()
        self.tcp_client.close()

    def fetch_impl(
            self, request: UDPRequest, callback: Callable[[UDPResponse], None]
    ) -> None:
        key = object()
        self.queue.append((key, request, callback))
        if not len(self.active) < self.max_clients:
            assert request.connect_timeout is not None
            assert request.request_timeout is not None
            timeout_handle = self.io_loop.add_timeout(
                self.io_loop.time()
                + min(request.connect_timeout, request.request_timeout),
                functools.partial(self._on_timeout, key, "in request queue"),
            )
        else:
            timeout_handle = None
        self.waiting[key] = (request, callback, timeout_handle)
        self._process_queue()
        if self.queue:
            gen_log.debug(
                "max_clients limit reached, request queued. "
                "%d active, %d queued requests." % (len(self.active), len(self.queue))
            )

    def _process_queue(self) -> None:
        while self.queue and len(self.active) < self.max_clients:
            key, request, callback = self.queue.popleft()
            if key not in self.waiting:
                continue
            self._remove_timeout(key)
            self.active[key] = (request, callback)
            release_callback = functools.partial(self._release_fetch, key)
            self._handle_request(request, release_callback, callback)

    def _connection_class(self) -> type:
        return _UDPConnection

    def _handle_request(
            self,
            request: UDPRequest,
            release_callback: Callable[[], None],
            final_callback: Callable[[UDPResponse], None],
    ) -> None:
        self._connection_class()(
            self,
            request,
            release_callback,
            final_callback,
            self.max_buffer_size,
            self.tcp_client,
            self.max_header_size,
            self.max_body_size,
        )

    def _release_fetch(self, key: object) -> None:
        del self.active[key]
        self._process_queue()

    def _remove_timeout(self, key: object) -> None:
        if key in self.waiting:
            request, callback, timeout_handle = self.waiting[key]
            if timeout_handle is not None:
                self.io_loop.remove_timeout(timeout_handle)
            del self.waiting[key]

    def _on_timeout(self, key: object, info: str = None) -> None:
        """Timeout callback of request.

        Construct a timeout HTTPResponse when a timeout occurs.

        :arg object key: A simple object to mark the request.
        :info string key: More detailed timeout information.
        """
        request, callback, timeout_handle = self.waiting[key]
        self.queue.remove((key, request, callback))

        error_message = "Timeout {0}".format(info) if info else "Timeout"
        timeout_response = UDPResponse(
            request,
            599,
            #error=HTTPTimeoutError(error_message),
            request_time=self.io_loop.time() - request.start_time,
        )
        self.io_loop.add_callback(callback, timeout_response)
        del self.waiting[key]


class _UDPConnection(object):
    _SUPPORTED_METHODS = set(
        ["GET", "HEAD", "POST", "PUT", "DELETE", "INIT", "OPTIONS"]
    )

    def __init__(
            self,
            client,
            request,
            release_callback,
            final_callback,
            udp_client,
            max_buffer_size):
        self.io_loop = IOLoop.current()
        self._timeout = None
        self.start_time = time.time()
        self.client = client
        self.request = request
        self.release_callback = release_callback
        self.final_callback = final_callback
        self.udp_client = udp_client
        self.max_buffer_size = max_buffer_size

        IOLoop.current().add_future(
            gen.convert_yielded(self.run()), lambda f: f.result()
        )

    async def run(self) -> None:
        try:
            self.parsed = urllib.parse.urlsplit(self.request.url)

            if self.parsed.scheme not in ("udp", "http"):
                raise ValueError("Unsupported url scheme: %s" % self.request.url)
            netloc = self.parsed.netloc
            if "@" in netloc:
                userpass, _, netloc = netloc.rpartition("@")
            host, port = httputil.split_host_and_port(netloc)
            if port is None:
                port = 443 if self.parsed.scheme == "https" else 80
            if re.match(r"^\[.*\]$", host):
                # raw ipv6 addresses in urls are enclosed in brackets
                host = host[1:-1]
            self.parsed_hostname = host  # save final host for _on_connect
            if self.request.allow_ipv6 is False:
                af = socket.AF_INET
            else:
                af = socket.AF_UNSPEC
            source_ip = None
            if self.request.network_interface:
                if is_valid_ip(self.request.network_interface):
                    source_ip = self.request.network_interface
                else:
                    raise ValueError(
                        "Unrecognized IPv4 or IPv6 address for network_interface, got %r"
                        % (self.request.network_interface,)
                    )

            timeout = min(self.request.connect_timeout, self.request.request_timeout)
            if timeout:
                self._timeout = self.io_loop.add_timeout(
                    self.start_time + timeout,
                    functools.partial(self._on_timeout, "while connecting"),
                )
                stream = await self.udp_client.connect(
                    host,
                    port,
                    af=af,
                    ssl_options=None,
                    max_buffer_size=self.max_buffer_size,
                    source_ip=source_ip,
                )

                if self.final_callback is None:
                    # final_callback is cleared if we've hit our timeout.
                    stream.close()
                    return
                self.stream = stream
                self.stream.set_close_callback(self.on_connection_close)
                self._remove_timeout()
                if self.final_callback is None:
                    return
                if self.request.request_timeout:
                    self._timeout = self.io_loop.add_timeout(
                        self.start_time + self.request.request_timeout,
                        functools.partial(self._on_timeout, "during request"),
                    )
                if (
                        self.request.method not in self._SUPPORTED_METHODS
                        and not self.request.allow_nonstandard_methods
                ):
                    raise KeyError("unknown method %s" % self.request.method)
                for key in (
                        "proxy_host",
                        "proxy_port",
                        "proxy_username",
                        "proxy_password",
                        "proxy_auth_mode",
                ):
                    if getattr(self.request, key, None):
                        raise NotImplementedError("%s not supported" % key)
                # if "Connection" not in self.request.headers:
                #     self.request.headers["Connection"] = "close"
                # if "Host" not in self.request.headers:
                #     if "@" in self.parsed.netloc:
                #         self.request.headers["Host"] = self.parsed.netloc.rpartition(
                #             "@"
                #         )[-1]
                #     else:
                #         self.request.headers["Host"] = self.parsed.netloc
                # username, password = None, None
                # if self.parsed.username is not None:
                #     username, password = self.parsed.username, self.parsed.password
                # elif self.request.auth_username is not None:
                #     username = self.request.auth_username
                #     password = self.request.auth_password or ""
                # if username is not None:
                #     assert password is not None
                #     if self.request.auth_mode not in (None, "basic"):
                #         raise ValueError(
                #             "unsupported auth_mode %s", self.request.auth_mode
                #         )
                #     self.request.headers["Authorization"] = "Basic " + _unicode(
                #         base64.b64encode(
                #             httputil.encode_username_password(username, password)
                #         )
                #     )
                # if self.request.user_agent:
                #     self.request.headers["User-Agent"] = self.request.user_agent
                # if not self.request.allow_nonstandard_methods:
                #     # Some HTTP methods nearly always have bodies while others
                #     # almost never do. Fail in this case unless the user has
                #     # opted out of sanity checks with allow_nonstandard_methods.
                #     body_expected = self.request.method in ("POST", "PATCH", "PUT")
                #     body_present = (
                #             self.request.body is not None
                #             or self.request.body_producer is not None
                #     )
                #     if (body_expected and not body_present) or (
                #             body_present and not body_expected
                #     ):
                #         raise ValueError(
                #             "Body must %sbe None for method %s (unless "
                #             "allow_nonstandard_methods is true)"
                #             % ("not " if body_expected else "", self.request.method)
                #         )
                # if self.request.expect_100_continue:
                #     self.request.headers["Expect"] = "100-continue"
                # if self.request.body is not None:
                #     # When body_producer is used the caller is responsible for
                #     # setting Content-Length (or else chunked encoding will be used).
                #     self.request.headers["Content-Length"] = str(len(self.request.body))
                # if (
                #         self.request.method == "POST"
                #         and "Content-Type" not in self.request.headers
                # ):
                #     self.request.headers[
                #         "Content-Type"
                #     ] = "application/x-www-form-urlencoded"
                # if self.request.decompress_response:
                #     self.request.headers["Accept-Encoding"] = "gzip"
                req_path = (self.parsed.path or "/") + (
                    ("?" + self.parsed.query) if self.parsed.query else ""
                )
                self.connection = self._create_connection(stream)
                start_line = httputil.RequestStartLine(
                    self.request.method, req_path, ""
                )
                self.connection.write_headers(start_line, self.request.headers)
                if self.request.expect_100_continue:
                    await self.connection.read_response(self)
                else:
                    await self._write_body(True)
        except Exception:
            if not self._handle_exception(*sys.exc_info()):
                raise
            # print(host, port, 'run')
            # addrinfo = socket.getaddrinfo(self.request.proxy_host, self.request.proxy_port,
            #                                                             socket.AF_INET,
            #                                                             socket.SOCK_DGRAM,
            #                                                             0, 0
            #                                                             )
            # 
            # af, socktype, proto, ai_canonname, sockaddr = addrinfo[0]
            # 
            # stream = IOStream(socket.socket(af, socktype, proto),
            #                        max_buffer_size=2500)
            # stream.connect(sockaddr)
        # addrinfo = socket.getaddrinfo(request.proxy_host, request.proxy_port,
        #                               socket.AF_INET,
        #                               socket.SOCK_DGRAM,
        #                               0, 0
        #                               )
        # af, socktype, proto, ai_canonname, sockaddr = addrinfo[0]
        # self.stream = IOStream(socket.socket(af, socktype, proto),
        #                        max_buffer_size=2500)
        # 
        # self.stream.connect(sockaddr).add_done_callback(self._on_connect)

        except Exception:
            if not self._handle_exception(*sys.exc_info()):
                raise

    async def _write_body(self, start_read: bool) -> None:
        if self.request.body is not None:
            self.connection.write(self.request.body)
        elif self.request.body_producer is not None:
            fut = self.request.body_producer(self.connection.write)
            if fut is not None:
                await fut
        self.connection.finish()
        if start_read:
            try:
                await self.connection.read_response(self)
            except StreamClosedError:
                if not self._handle_exception(*sys.exc_info()):
                    raise

    def _on_timeout(self, info: str = None) -> None:
        """Timeout callback of _HTTPConnection instance.

        Raise a `HTTPTimeoutError` when a timeout occurs.

        :info string key: More detailed timeout information.
        """
        self._timeout = None
        error_message = "Timeout {0}".format(info) if info else "Timeout"
        if self.final_callback is not None:
            print('raise')

            # self._handle_exception(
            #     HTTPTimeoutError, HTTPTimeoutError(error_message), None
            # )

    def _on_connect(self, future):
        if future.done():
            self.stream.write(b'init').add_done_callback(self._on_response)

    def _handle_exception(
            self,
            typ: "Optional[Type[BaseException]]",
            value: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> bool:
        if self.final_callback:
            self._remove_timeout()
            if isinstance(value, StreamClosedError):
                if value.real_error is None:
                    value = UDPStreamClosedError("Stream closed")
                else:
                    value = value.real_error
            self._run_callback(
                UDPResponse(
                    self.request,
                    599,
                    error=value,
                    request_time=self.io_loop.time() - self.start_time,
                    start_time=self.start_time,
                )
            )

            if hasattr(self, "stream"):
                # TODO: this may cause a StreamClosedError to be raised
                # by the connection's Future.  Should we cancel the
                # connection more gracefully?
                self.stream.close()
            return True
        else:
            # If our callback has already been called, we are probably
            # catching an exception that is not caused by us but rather
            # some child of our callback. Rather than drop it on the floor,
            # pass it along, unless it's just the stream being closed.
            return isinstance(value, StreamClosedError)

    def _on_response(self, future):
        print(future)
        # print(future)
        return future
        # if self.release_callback is not None:
        #     release_callback = self.release_callback
        #     self.release_callback = None
        #     release_callback()
        # self.stream.close()

    def _remove_timeout(self) -> None:
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _run_callback(self, response: UDPResponse) -> None:
        self._release()
        if self.final_callback is not None:
            final_callback = self.final_callback
            self.final_callback = None  # type: ignore
            self.io_loop.add_callback(final_callback, response)

    def _release(self) -> None:
        if self.release_callback is not None:
            release_callback = self.release_callback
            self.release_callback = None  # type: ignore
            release_callback()