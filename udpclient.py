#!/usr/bin/env python
# encoding: utf-8

import functools
import socket
import numbers
import datetime

from tornado.concurrent import Future, future_add_done_callback
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream
from tornado import gen
from tornado.netutil import Resolver
from tornado.platform.auto import set_close_exec
from tornado.gen import TimeoutError

import typing
from typing import Any, Union, Dict, Tuple, List, Callable, Iterator

if typing.TYPE_CHECKING:
    from typing import Optional, Set  # noqa: F401

_INITIAL_CONNECT_TIMEOUT = 0.3


class _Connector(object):
    def __init__(
            self,
            addrinfo: List[Tuple],
            connect: Callable[
                [socket.AddressFamily, Tuple], Tuple[IOStream, "Future[IOStream]"]
            ],
    ) -> None:
        print(connect, addrinfo)
        self.io_loop = IOLoop.current()
        self.connect = connect
        self.future = (Future())  # type: Future[Tuple[socket.AddressFamily, Any, IOStream]]

        self.timeout = None  # type: Optional[object]
        self.connect_timeout = None  # type: Optional[object]
        self.last_error = None  # type: Optional[Exception]
        self.remaining = len(addrinfo)
        self.primary_addrs, self.secondary_addrs = self.split(addrinfo)
        self.streams = set()  # type: Set[IOStream]

    @staticmethod
    def split(
            addrinfo: List[Tuple]
    ) -> Tuple[
        List[Tuple[socket.AddressFamily, Tuple]],
        List[Tuple[socket.AddressFamily, Tuple]],
    ]:
        primary = []
        secondary = []
        primary_af = addrinfo[0][0]
        for af, addr in addrinfo:
            if af == primary_af:
                primary.append((af, addr))
            else:
                secondary.append((af, addr))
        return primary, secondary

    def start(
            self,
            timeout: float = _INITIAL_CONNECT_TIMEOUT,
            connect_timeout: Union[float, datetime.timedelta] = None,
    ) -> "Future[Tuple[socket.AddressFamily, Any, socket.SOCK_DGRAM]]":
        self.try_connect(iter(self.primary_addrs))
        self.set_timeout(timeout)
        if connect_timeout is not None:
            self.set_connect_timeout(connect_timeout)
        return self.future

    def try_connect(self, addrs: Iterator[Tuple[socket.AddressFamily, Tuple]]) -> None:
        try:
            af, addr = next(addrs)
        except StopIteration:
            # We've reached the end of our queue, but the other queue
            # might still be working.  Send a final error on the future
            # only when both queues are finished.
            if self.remaining == 0 and not self.future.done():
                self.future.set_exception(
                    self.last_error or IOError("connection failed")
                )
            return
        stream, future = self.connect(af, addr)
        self.streams.add(stream)
        future_add_done_callback(
            future, functools.partial(self.on_connect_done, addrs, af, addr)
        )

    def on_connect_done(
            self,
            addrs: Iterator[Tuple[socket.AddressFamily, Tuple]],
            af: socket.AddressFamily,
            addr: Tuple,
            future: "Future[IOStream]",
    ) -> None:
        self.remaining -= 1
        try:
            stream = future.result()
        except Exception as e:
            if self.future.done():
                return
            # Error: try again (but remember what happened so we have an
            # error to raise in the end)
            self.last_error = e
            self.try_connect(addrs)
            if self.timeout is not None:
                # If the first attempt failed, don't wait for the
                # timeout to try an address from the secondary queue.
                self.io_loop.remove_timeout(self.timeout)
                self.on_timeout()
            return
        self.clear_timeouts()
        if self.future.done():
            # This is a late arrival; just drop it.
            stream.close()
        else:
            self.streams.discard(stream)
            self.future.set_result((af, addr, stream))
            self.close_streams()

    def set_timeout(self, timeout: float) -> None:
        self.timeout = self.io_loop.add_timeout(
            self.io_loop.time() + timeout, self.on_timeout
        )

    def on_timeout(self) -> None:
        self.timeout = None
        if not self.future.done():
            self.try_connect(iter(self.secondary_addrs))

    def clear_timeout(self) -> None:
        if self.timeout is not None:
            self.io_loop.remove_timeout(self.timeout)

    def set_connect_timeout(
            self, connect_timeout: Union[float, datetime.timedelta]
    ) -> None:
        self.connect_timeout = self.io_loop.add_timeout(
            connect_timeout, self.on_connect_timeout
        )

    def on_connect_timeout(self) -> None:
        if not self.future.done():
            self.future.set_exception(TimeoutError())
        self.close_streams()

    def clear_timeouts(self) -> None:
        if self.timeout is not None:
            self.io_loop.remove_timeout(self.timeout)
        if self.connect_timeout is not None:
            self.io_loop.remove_timeout(self.connect_timeout)

    def close_streams(self) -> None:
        for stream in self.streams:
            stream.close()


class UDPClient(object):
    """A non-blocking UDP connection factory.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.
    """

    def __init__(self, resolver: Resolver = None) -> None:
        if resolver is not None:
            self.resolver = resolver
            self._own_resolver = False
        else:
            self.resolver = Resolver()
            self._own_resolver = True

    # self.io_loop = io_loop or IOLoop.current()

    # def __init__(self, resolver: Resolver = None) -> None:
    #     if resolver is not None:
    #         self.resolver = resolver
    #         self._own_resolver = False
    #     else:
    #         self.resolver = Resolver()
    #         self._own_resolver = True

    def close(self) -> None:
        if self._own_resolver:
            self.resolver.close()

    def connect(
            self,
            host: str,
            port: int,
            af: socket.AddressFamily = socket.AF_UNSPEC,
            ssl_options=None,
            max_buffer_size: int = None,
            source_ip: str = None,
            source_port: int = None,
            timeout: Union[float, datetime.timedelta] = None,
    ) -> IOStream:

        if timeout is not None:
            if isinstance(timeout, numbers.Real):
                timeout = IOLoop.current().time() + timeout
            elif isinstance(timeout, datetime.timedelta):
                timeout = IOLoop.current().time() + timeout.total_seconds()
            else:
                raise TypeError("Unsupported timeout %r" % timeout)
        if timeout is not None:
            addrinfo = gen.with_timeout(
                timeout, self.resolver.resolve(host, port, af)
            )
        else:
            addrinfo = self.resolver.resolve(host, port, af)
        # noinspection PyTypeChecker
        connector = _Connector(addrinfo, functools.partial(self._create_stream, max_buffer_size, source_ip=source_ip,
                                                           source_port=source_port, ), )
        af, addr, stream = connector.start(connect_timeout=timeout)
        # TODO: For better performance we could cache the (af, addr)
        # information here and re-use it on subsequent connections to
        # the same host. (http://tools.ietf.org/html/rfc6555#section-4.2)
        if ssl_options is not None:
            if timeout is not None:
                stream = gen.with_timeout(
                    timeout,
                    stream.start_tls(
                        False, ssl_options=ssl_options, server_hostname=host
                    ),
                )
            else:
                stream = stream.start_tls(
                    False, ssl_options=ssl_options, server_hostname=host
                )
        return stream

        # sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 
        # sock.bind((host, port))
        # 
        # # if broadcast:
        # #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # # 
        # # if reuse:
        # #     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 
        # if timeout is not None:
        #     if isinstance(timeout, numbers.Real):
        #         timeout = IOLoop.current().time() + timeout
        #     elif isinstance(timeout, datetime.timedelta):
        #         timeout = IOLoop.current().time() + timeout.total_seconds()
        #     else:
        #         raise TypeError("Unsupported timeout %r" % timeout)
        # if timeout is not None:
        #     addrinfo = gen.with_timeout(
        #         timeout, self.resolver.resolve(host, port, af)
        #     )
        # else:
        #     addrinfo = self.resolver.resolve(host, port, af)
        #     connector = _Connector(
        #         
        #         addrinfo,
        #         functools.partial(
        #             self._create_stream,
        #             max_buffer_size,
        #             source_ip=source_ip,
        #             source_port=source_port,
        #         ),
        #     )
        # af, addr, stream = connector.start(connect_timeout=timeout)
        # 
        # # stream = UDPStream(
        # #     socket=sock,
        # #     max_buffer_size=max_buffer_size,
        # #     destination=(host, port),
        # # )
        # # 
        # # if ssl_options is not None:
        # #     stream = yield stream.start_tls(
        # #         server_side=False,
        # #         ssl_options=ssl_options,
        # #         server_hostname=host,
        # #     )
        # 
        # return stream

    # async def connect(
    #     self,
    #     host: str,
    #     port: int,
    #     af: socket.AddressFamily = socket.AF_UNSPEC,
    #     # ssl_options: Union[Dict[str, Any], ssl.SSLContext] = None,
    #     max_buffer_size: int = None,
    #     source_ip: str = None,
    #     source_port: int = None,
    #     timeout: Union[float, datetime.timedelta] = None,
    # ) -> IOStream:
    #     if timeout is not None:
    #         if isinstance(timeout, numbers.Real):
    #             timeout = IOLoop.current().time() + timeout
    #         elif isinstance(timeout, datetime.timedelta):
    #             timeout = IOLoop.current().time() + timeout.total_seconds()
    #         else:
    #             raise TypeError("Unsupported timeout %r" % timeout)
    #     if timeout is not None:
    #         addrinfo = await gen.with_timeout(
    #             timeout, self.resolver.resolve(host, port, af)
    #         )
    #     else:
    #         addrinfo = await self.resolver.resolve(host, port, af)
    #     connector = _Connector(
    #         addrinfo,
    #         functools.partial(
    #             self._create_stream,
    #             max_buffer_size,
    #             source_ip=source_ip,
    #             source_port=source_port,
    #         ),
    #     )
    #     af, addr, stream = await connector.start(connect_timeout=timeout)
    #     # TODO: For better performance we could cache the (af, addr)
    #     # information here and re-use it on subsequent connections to
    #     # the same host. (http://tools.ietf.org/html/rfc6555#section-4.2)
    #     if ssl_options is not None:
    #         if timeout is not None:
    #             stream = await gen.with_timeout(
    #                 timeout,
    #                 stream.start_tls(
    #                     False, ssl_options=ssl_options, server_hostname=host
    #                 ),
    #             )
    #         else:
    #             stream = await stream.start_tls(
    #                 False, ssl_options=ssl_options, server_hostname=host
    #             )
    #     return stream

    def _create_stream(
            self,
            max_buffer_size: int,
            af: socket.AddressFamily,
            addr: Tuple,
            source_ip: str = None,
            source_port: int = None,
    ) -> Tuple[IOStream, "Future[IOStream]"]:

        # Always connect in plaintext; we'll convert to ssl if necessary
        # after one connection has completed.
        source_port_bind = source_port if isinstance(source_port, int) else 0
        source_ip_bind = source_ip
        if source_port_bind and not source_ip:
            # User required a specific port, but did not specify
            # a certain source IP, will bind to the default loopback.
            source_ip_bind = "::1" if af == socket.AF_INET6 else "127.0.0.1"
            # Trying to use the same address family as the requested af socket:
            # - 127.0.0.1 for IPv4
            # - ::1 for IPv6
        socket_obj = socket.socket(af)
        set_close_exec(socket_obj.fileno())
        if source_port_bind or source_ip_bind:
            # If the user requires binding also to a specific IP/port.
            try:
                socket_obj.bind((source_ip_bind, source_port_bind))
            except socket.error:
                socket_obj.close()
                # Fail loudly if unable to use the IP/port.
                raise
        try:
            stream = IOStream(socket_obj, max_buffer_size=max_buffer_size)
        except socket.error as e:
            fu = Future()  # type: Future[IOStream]
            fu.set_exception(e)
            return stream, fu
        else:
            return stream, stream.connect(addr)


