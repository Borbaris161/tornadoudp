"""A non-blocking, single-threaded UDP server."""

from __future__ import absolute_import, division, print_function, with_statement

import errno
import os
import socket

from tornado.ioloop import IOLoop
from tornado import process, httputil
from tornado.netutil import set_close_exec
from tornado.httputil import HTTPServerRequest

class UDPServer(object):
    def __init__(self, io_loop=None):
        self.io_loop = io_loop
        self._sockets = {}  # fd -> socket object
        self._pending_sockets = []
        self._started = False

    def add_sockets(self, sockets):
        if self.io_loop is None:
            self.io_loop = IOLoop.instance()

        for sock in sockets:
            self._sockets[sock.fileno()] = sock
            add_accept_handler(sock, self._on_recive,
                               io_loop=self.io_loop)

    def bind(self, port=None, address=None, family=socket.AF_UNSPEC, backlog=25):
        sockets = bind_sockets(port, address=address, family=family,
                               backlog=backlog)

        if self._started:
            self.add_sockets(sockets)
        else:
            self._pending_sockets.extend(sockets)

    def start(self, num_processes=1):
        assert not self._started
        self._started = True
        if num_processes != 1:
            process.fork_processes(num_processes)
        sockets = self._pending_sockets
        self._pending_sockets = []
        self.add_sockets(sockets)

    def stop(self):
        for fd, sock in self._sockets.iteritems():
            self.io_loop.remove_handler(fd)
            sock.close()


    def _on_recive(self, data, address):
        print(data)
        host = address[0]
        port = address[1]
        # sock = socket.socket(
        #     socket.AF_INET,
        #     socket.SOCK_DGRAM)
        # sock.sendto(data, ("127.0.0.1", 1111))
        # bufferSize = 1024
        # msgFromServer = sock.recvfrom(bufferSize)
        # msg = "Message from Server {}".format(msgFromServer[0])


def bind_sockets(port, address=None, family=socket.AF_UNSPEC, backlog=25):
    sockets = []
    if address == "":
        address = None
    flags = socket.AI_PASSIVE
    if hasattr(socket, "AI_ADDRCONFIG"):
        flags |= socket.AI_ADDRCONFIG

    for res in set(socket.getaddrinfo(address,
                                      port,
                                      family,
                                      socket.SOCK_DGRAM,
                                      0, flags)):

        af, socktype, proto, canonname, sockaddr = res
        sock = socket.socket(af, socktype, proto)
        set_close_exec(sock.fileno())
        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if af == socket.AF_INET6:
            if hasattr(socket, "IPPROTO_IPV6"):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock.setblocking(0)
        sock.bind(sockaddr)
        sockets.append(sock)
    return sockets


def add_accept_handler(sock, callback, io_loop=None):
    if io_loop is None:
        io_loop = IOLoop.instance()
    def handle_accept(fd, events):
        print(fd, events)
        while True:
            try:
                data, address = sock.recvfrom(1024)
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return
                raise
            callback(data, address)
    io_loop.add_handler(sock.fileno(), handle_accept, IOLoop.READ)


class UDPRequestHandler(object):
    SUPPORTED_METHODS = ("GET", "INIT", "POST", "DELETE", "PATCH", "PUT", "OPTIONS")

    def __init__(
            self,
            application,
            request: httputil.HTTPServerRequest,
            **kwargs
    ):
        super(UDPRequestHandler, self).__init__()

        self.application = application
        self.request = request
        self._headers_written = False
        self._finished = False
        self._auto_finish = True
        self._prepared_future = None
        assert self.request.connection is not None
        self.request.connection.set_close_callback(  # type: ignore
            self.on_connection_close
        )
        self.initialize(**kwargs)  # type: ignore

    def _initialize(self) -> None:
        pass

    def get(self, *args) -> None:
        to_url = self._url.format(*args)
        if self.request.query_arguments:
            # TODO: figure out typing for the next line.
            to_url = httputil.url_concat(
                to_url,
                list(httputil.qs_to_qsl(self.request.query_arguments)),  # type: ignore
            )
        self.redirect(to_url, permanent=self._permanent)

    def init(self):
        pass



# import socket
server = UDPServer()
server.bind(port=1111, address='10.1.4.6')
server.start(1)
IOLoop.instance().start()


from tornado.web import RequestHandler, Application


# if hasattr(socket, 'AF_UNIX'):
#     def bind_unix_socket(file, mode=600, backlog=128):
#         sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
#         set_close_exec(sock.fileno())
#         sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         sock.setblocking(0)
#         try:
#             st = os.stat(file)
#         except OSError as err:
#             if err.errno != errno.ENOENT:
#                 raise
#             else:
#                 if st.S_ISSOCK(st.st_mode):
#                     os.remove(file)
#                 else:
#                     raise ValueError("File %s exists and is not a socket", file)
#         sock.bind(file)
#         os.chmod(file, mode)
#         sock.listen(backlog)
#         return sock