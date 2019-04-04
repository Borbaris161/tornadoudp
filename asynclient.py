#!/usr/bin/env python
# encoding: utf-8

import datetime
import functools

import ssl
import time
import weakref

from typing import Awaitable, cast

from typing import Dict, Any, Callable, Optional, Type, Union

from tornado import httputil, gen
from tornado.concurrent import Future, future_set_result_unless_cancelled, \
    future_set_exception_unless_cancelled

from tornado.ioloop import IOLoop
from tornado.util import unicode_type, Configurable

from tornado.escape import utf8, native_str

# curl_log = logging.getLogger("tornado.curl_udpclient")

_INITIAL_CONNECT_TIMEOUT = 0.3

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


class UDPClient(object):

    def __init__(
        self, async_client_class: Type["AsyncUDPClient"] = None, **kwargs: Any
    ) -> None:
        # Initialize self._closed at the beginning of the constructor
        # so that an exception raised here doesn't lead to confusing
        # failures in __del__.
        self._closed = True
        self._io_loop = IOLoop(make_current=False)
        if async_client_class is None:
            async_client_class = AsyncUDPClient

        # Create the client while our IOLoop is "current", without
        # clobbering the thread's real current IOLoop (if any).
        async def make_client() -> "AsyncUDPClient":
            await gen.sleep(0)
            assert async_client_class is not None
            return async_client_class(**kwargs)

        # def make_client() -> "AsyncUDPClient":
        #     gen_sleep = gen.sleep(0)
        #     assert async_client_class is not None
        #     return async_client_class(**kwargs)


        self._async_client = self._io_loop.run_sync(make_client)
        self._closed = False

    def __del__(self) -> None:
        self.close()

    def close(self) -> None:
        """Closes the HTTPClient, freeing any resources used."""
        if not self._closed:
            self._async_client.close()
            self._io_loop.close()
            self._closed = True

    def fetch(
        self, request: Union["UDPRequest", str], **kwargs: Any
    ) -> "UDPResponse":
        print(self._async_client)
        response = self._io_loop.run_sync(
            functools.partial(self._async_client.fetch, request, **kwargs)
        )
        return response

# class UDPStream(IOStream):
#     _destination = None
#
#     def __init__(self, destination, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._destination = destination
#
#     def write_to_fd(self, data):
#         # A dirty hack. Do not use 'socket.connect()' & 'socket.send()',
#         # as dectination can be a broadcast and a socket 'connected' to
#         # broadcast will not be able to receive data.
#         return self.socket.sendto(data, self._destination)

class UDPRequest(object):
    """HTTP client request object."""

    _headers = None  # type: Union[Dict[str, str], httputil.HTTPHeaders]

    _DEFAULTS = dict(
        connect_timeout=20.0,
        request_timeout=20.0,
        follow_redirects=True,
        max_redirects=5,
        decompress_response=True,
        proxy_password="",
        allow_nonstandard_methods=False,
        validate_cert=True,
    )

    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Union[Dict[str, str], httputil.HTTPHeaders] = None,
        body: Union[bytes, str] = None,
        auth_username: str = None,
        auth_password: str = None,
        auth_mode: str = None,
        connect_timeout: float = None,
        request_timeout: float = None,
        if_modified_since: Union[float, datetime.datetime] = None,
        follow_redirects: bool = None,
        max_redirects: int = None,
        user_agent: str = None,
        use_gzip: bool = None,
        network_interface: str = None,
        streaming_callback: Callable[[bytes], None] = None,
        header_callback: Callable[[str], None] = None,
        prepare_curl_callback: Callable[[Any], None] = None,
        proxy_host: str = None,
        proxy_port: int = None,
        proxy_username: str = None,
        proxy_password: str = None,
        proxy_auth_mode: str = None,
        allow_nonstandard_methods: bool = None,
        validate_cert: bool = None,
        ca_certs: str = None,
        allow_ipv6: bool = None,
        client_key: str = None,
        client_cert: str = None,
        body_producer: Callable[[Callable[[bytes], None]], "Future[None]"] = None,
        expect_100_continue: bool = False,
        decompress_response: bool = None,
        ssl_options: Union[Dict[str, Any], ssl.SSLContext] = None,
    ) -> None:
        # Note that some of these attributes go through property setters
        # defined below.
        self.headers = headers
        if if_modified_since:
            self.headers["If-Modified-Since"] = httputil.format_timestamp(
                if_modified_since
            )
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.proxy_auth_mode = proxy_auth_mode
        self.url = url
        self.method = method
        self.body = body
        self.body_producer = body_producer
        self.auth_username = auth_username
        self.auth_password = auth_password
        self.auth_mode = auth_mode
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.follow_redirects = follow_redirects
        self.max_redirects = max_redirects
        self.user_agent = user_agent
        if decompress_response is not None:
            self.decompress_response = decompress_response  # type: Optional[bool]
        else:
            self.decompress_response = use_gzip
        self.network_interface = network_interface
        self.streaming_callback = streaming_callback
        self.header_callback = header_callback
        self.prepare_curl_callback = prepare_curl_callback
        self.allow_nonstandard_methods = allow_nonstandard_methods
        self.validate_cert = validate_cert
        self.ca_certs = ca_certs
        self.allow_ipv6 = allow_ipv6
        self.client_key = client_key
        self.client_cert = client_cert
        self.ssl_options = ssl_options
        self.expect_100_continue = expect_100_continue
        self.start_time = time.time()
        
        
    @property
    def headers(self) -> httputil.HTTPHeaders:
        # TODO: headers may actually be a plain dict until fairly late in
        # the process (AsyncHTTPClient.fetch), but practically speaking,
        # whenever the property is used they're already HTTPHeaders.
        return self._headers  # type: ignore

    @headers.setter
    def headers(self, value: Union[Dict[str, str], httputil.HTTPHeaders]) -> None:
        if value is None:
            self._headers = httputil.HTTPHeaders()
        else:
            self._headers = value  # type: ignore

    @property
    def body(self) -> bytes:
        return self._body

    @body.setter
    def body(self, value: Union[bytes, str]) -> None:
        self._body = utf8(value)

class UDPResponse(object):
    error = None  # type: Optional[BaseException]
    _error_is_response_code = False
    request = None  # type: HTTPRequest

    def __init__(
            self,
            request: UDPRequest,
            code: int,
            effective_url: str = None,
            error: BaseException = None,
            request_time: float = None,
            time_info: Dict[str, float] = None,
            reason: str = None,
            start_time: float = None,
    ) -> None:

        if isinstance(request, _RequestProxy):
            self.request = request.request
        else:
            self.request = request
        self.code = code
        self.reason = reason
        self._body = None  # type: Optional[bytes]
        if effective_url is None:
            self.effective_url = request.url
        else:
            self.effective_url = effective_url
        self._error_is_response_code = False
        if error is None:
            if self.code < 200 or self.code >= 300:
                self._error_is_response_code = True
                self.error = UDPClientError(self.code, message=self.reason, response=self)
            else:
                self.error = None
        else:
            self.error = error
        self.start_time = start_time
        self.request_time = request_time
        self.time_info = time_info or {}

    @property
    def body(self) -> bytes:
        if self.buffer is None:
            raise ValueError("body not set")
        elif self._body is None:
            self._body = self.buffer.getvalue()

        return self._body

    def rethrow(self) -> None:
        """If there was an error on the request, raise an `HTTPError`."""
        if self.error:
            raise self.error

    def __repr__(self) -> str:
        args = ",".join("%s=%r" % i for i in sorted(self.__dict__.items()))
        return "%s(%s)" % (self.__class__.__name__, args)


class AsyncUDPClient(Configurable):


    _instance_cache = None  # type: Dict[IOLoop, AsyncUDPClient]

    @classmethod
    def configurable_base(cls) -> Type[Configurable]:
        return AsyncUDPClient

    @classmethod
    def configurable_default(cls) -> Type[Configurable]:

        from simple_udpclient import SimpleAsyncUDPClient

        return SimpleAsyncUDPClient

    @classmethod
    def _async_clients(cls) -> Dict[IOLoop, "AsyncUDPClient"]:
        attr_name = "_async_client_dict_" + cls.__name__
        if not hasattr(cls, attr_name):
            setattr(cls, attr_name, weakref.WeakKeyDictionary())
        return getattr(cls, attr_name)

    def __new__(cls, force_instance: bool = False, **kwargs: Any) -> "AsyncUDPClient":
        io_loop = IOLoop.current()
        if force_instance:
            instance_cache = None
        else:
            instance_cache = cls._async_clients()
        if instance_cache is not None and io_loop in instance_cache:
            return instance_cache[io_loop]
        instance = super(AsyncUDPClient, cls).__new__(cls, **kwargs)  # type: ignore
        instance._instance_cache = instance_cache
        if instance_cache is not None:
            instance_cache[instance.io_loop] = instance
        return instance

    def initialize(self, defaults: Dict[str, Any] = None) -> None:
        self.io_loop = IOLoop.current()
        self.defaults = dict(UDPRequest._DEFAULTS)
        if defaults is not None:
            self.defaults.update(defaults)
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._instance_cache is not None:
            cached_val = self._instance_cache.pop(self.io_loop, None)
            if cached_val is not None and cached_val is not self:
                raise RuntimeError("inconsistent AsyncHTTPClient cache")

    def fetch(
            self,
            request: Union[str, "UDPRequest"],
            raise_error: bool = True,
            **kwargs: Any
    ) -> Awaitable["UDPResponse"]:

        if self._closed:
            raise RuntimeError("fetch() called on closed AsyncUDPClient")
        if not isinstance(request, UDPRequest):
            request = UDPRequest(url=request, **kwargs)
        else:
            if kwargs:
                raise ValueError(
                    "kwargs can't be used if request is an UDPRequest object"
                )

        request.headers = httputil.HTTPHeaders(request.headers)
        request_proxy = _RequestProxy(request, self.defaults)
        future = Future()  # type: Future[UDPResponse]

        def handle_response(response: "UDPResponse") -> None:
            if response.error:
                if raise_error or not response._error_is_response_code:
                    future_set_exception_unless_cancelled(future, response.error)
                    return
            future_set_result_unless_cancelled(future, response)

        self.fetch_impl(cast(UDPRequest, request_proxy), handle_response)
        return future

    def fetch_impl(
            self, request: "UDPRequest", callback: Callable[["UDPResponse"], None]
    ) -> None:

        raise NotImplementedError()

    @classmethod
    def configure(
            cls, impl: "Union[None, str, Type[Configurable]]", **kwargs: Any
    ) -> None:
        super(AsyncUDPClient, cls).configure(impl, **kwargs)

class _RequestProxy(object):

    def __init__(
        self, request: UDPRequest, defaults: Optional[Dict[str, Any]]
    ) -> None:
        self.request = request
        self.defaults = defaults

    def __getattr__(self, name: str) -> Any:
        request_attr = getattr(self.request, name)
        if request_attr is not None:
            return request_attr
        elif self.defaults is not None:
            return self.defaults.get(name, None)
        else:
            return None

class UDPClientError(Exception):

    def __init__(
        self, code: int, message: str = None, response: UDPResponse = None
    ) -> None:
        self.code = code
        self.message = message or httputil.responses.get(code, "Unknown")
        self.response = response
        super(UDPClientError, self).__init__(code, message, response)

    def __str__(self) -> str:
        return "HTTP %d: %s" % (self.code, self.message)

    # There is a cyclic reference between self and self.response,
    # which breaks the default __repr__ implementation.
    # (especially on pypy, which doesn't have the same recursion
    # detection as cpython).
    __repr__ = __str__

class UDPStreamClosedError(UDPClientError):
    """Error raised by SimpleAsyncHTTPClient when the underlying stream is closed.

    When a more specific exception is available (such as `ConnectionResetError`),
    it may be raised instead of this one.

    For historical reasons, this is a subclass of `.HTTPClientError`
    which simulates a response code of 599.

    .. versionadded:: 5.1
    """

    def __init__(self, message: str) -> None:
        super(UDPStreamClosedError, self).__init__(599, message=message)

    def __str__(self) -> str:
        return self.message or "Stream closed"


def main() -> None:
    from tornado.options import define, options, parse_command_line

    define("print_headers", type=bool, default=False)
    define("print_body", type=bool, default=True)
    define("follow_redirects", type=bool, default=True)
    define("validate_cert", type=bool, default=True)
    define("proxy_host", type=str)
    define("proxy_port", type=int)
    args = parse_command_line()
    client = UDPClient()
    for arg in args:
        try:
            response = client.fetch(
                arg,
                follow_redirects=options.follow_redirects,
                validate_cert=options.validate_cert,
                proxy_host=options.proxy_host,
                proxy_port=options.proxy_port,
            )
        except UDPClientError as e:
            if e.response is not None:
                response = e.response
            else:
                raise
        if options.print_headers:
            print(response.headers)
        if options.print_body:
            print(native_str(response.body))
    client.close()


if __name__ == "__main__":
    main()

#






