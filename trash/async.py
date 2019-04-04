class AsyncUDPClient(object):

    def __init__(self, io_loop=None):
        self.io_loop = io_loop or IOLoop.instance()
        self.max_clients = 10
        self.queue = collections.deque()
        self.active = {}
        self.max_buffer_size = 2500
        self._closed = False
        self.defaults = dict(UDPRequest._DEFAULTS)
        # self.max_clients = max_clients
        self.resolver = Resolver()
        self.queue = (
            collections.deque()
        )  # type: Deque[Tuple[object, HTTPRequest, Callable[[HTTPResponse], None]]]
        self.active = (
            {}
        )  # type: Dict[object, Tuple[HTTPRequest, Callable[[HTTPResponse], None]]]
        self.waiting = (
            {}
        )  # type: Dict[object, Tuple[HTTPRequest, Callable[[HTTPResponse], None], object]]
        # self.max_buffer_size = max_buffer_size
        # self.max_header_size = max_header_size
        # self.max_body_size = max_body_size
        # resolver = self.resolver
        self.network_interface = self._curl_create()
        self.udp_client = UDPClient(resolver=self.resolver)



    def fetch(
        self,
        request: Union[str, "UDPRequest"],
        raise_error: bool = True, **kwargs: Any
        )-> Awaitable["UDPResponse"]:

        if self._closed:
            raise RuntimeError("fetch() called on closed AsyncUDPClient")
        if not isinstance(request, UDPRequest):
            request = UDPRequest(url=request, **kwargs)
        else:
            if kwargs:
                raise ValueError(
                    "kwargs can't be used if request is an HTTPRequest object"
                )
        request_proxy = _RequestProxy(request, self.defaults)
        future = Future()  # type: Future[UDPResponse]

        def handle_response(response: "UDPResponse") -> None:
            # if response.error:
            #     if raise_error or not response._error_is_response_code:
            #         future_set_exception_unless_cancelled(future, response.error)
            #         return
            future_set_result_unless_cancelled(future, response)
        self.fetch_impl(cast(UDPRequest, request_proxy), handle_response)
        return future


    def _set_timeout(self, msecs: int) -> None:
        """Called by libcurl to schedule a timeout."""
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
        self._timeout = self.io_loop.add_timeout(
            self.io_loop.time() + msecs / 1000.0, self._handle_timeout
        )

    def fetch_impl(
            self, request: UDPRequest, 
            callback: Callable[[UDPResponse], None]
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
        # if self.queue:
        #     gen_log.debug(
        #         "max_clients limit reached, request queued. "
        #         "%d active, %d queued requests." % (len(self.active), len(self.queue))
        #     )
       # self._set_timeout(0)


    def _process_queue(self):
        while self.queue and len(self.active) < self.max_clients:
            key, request, callback = self.queue.popleft()
            key = object()
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
            self.udp_client,
            self.max_buffer_size
        )


    def _release_fetch(self, key):
        del self.active[key]
        self._process_queue()