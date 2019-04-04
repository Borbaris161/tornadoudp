import sys
import threading

from tornado.util import raise_exc_info


class StackContextInconsistentError(Exception):
    pass


class _State(threading.local):
    def __init__(self):
        self.contexts = (tuple(), None)


_state = _State()


class StackContext(object):
    def __init__(self, context_factory):
        self.context_factory = context_factory
        self.contexts = []

    # protocol
    def enter(self):
        context = self.context_factory()
        self.contexts.append(context)
        context.__enter__()

    def exit(self, type, value, traceback):
        context = self.contexts.pop()
        context.__exit__(type, value, traceback)

    # with statement
    def __enter__(self):
        self.old_contexts = _state.contexts
        self.new_contexts = (self.old_contexts[0] + (self,), self)
        _state.contexts = self.new_contexts

        try:
            self.enter()
        except:
            _state.contexts = self.old_contexts
            raise

        return lambda: True

    def __exit__(self, type, value, traceback):
        try:
            self.exit(type, value, traceback)
        finally:
            final_contexts = _state.contexts
            _state.contexts = self.old_contexts

            if final_contexts != self.new_contexts:
                raise StackContextInconsistentError()


class ExceptionStackContext(object):
    def __init__(self, exception_handler):
        self.exception_handler = exception_handler

    # protocol
    def exit(self, type, value, traceback):
        if type is not None:
            return self.exception_handler(type, value, traceback)

    # with statement
    def __enter__(self):
        self.old_contexts = _state.contexts
        self.new_contexts = (self.old_contexts[0], self)
        _state.contexts = self.new_contexts

        return lambda: True

    def __exit__(self, type, value, traceback):
        try:
            if type is not None:
                return self.exception_handler(type, value, traceback)
        finally:
            final_contexts = _state.contexts
            _state.contexts = self.old_contexts

            if final_contexts != self.new_contexts:
                raise StackContextInconsistentError()


class NullContext(object):
    def __enter__(self):
        self.old_contexts = _state.contexts
        _state.contexts = (tuple(), None)

    def __exit__(self, type, value, traceback):
        _state.contexts = self.old_contexts


def wrap(fn):
    # Check if function is already wrapped
    if fn is None or hasattr(fn, '_wrapped'):
        return fn

    # Capture current stack head
    contexts = _state.contexts

    #@functools.wraps
    def wrapped(*args, **kwargs):
        try:
            # Force local state - switch to new stack chain
            current_state = _state.contexts
            _state.contexts = contexts

            # Current exception
            exc = (None, None, None)
            top = None

            # Apply stack contexts
            last_ctx = 0
            stack = contexts[0]

            # Apply state
            for n in stack:
                try:
                    n.enter()
                    last_ctx += 1
                except:
                    # Exception happened. Record exception info and store top-most handler
                    exc = sys.exc_info()
                    top = n.old_contexts[1]

            # Execute callback if no exception happened while restoring state
            if top is None:
                try:
                    fn(*args, **kwargs)
                except:
                    exc = sys.exc_info()
                    top = contexts[1]

            # If there was exception, try to handle it by going through the exception chain
            if top is not None:
                exc = _handle_exception(top, exc)
            else:
                # Otherwise take shorter path and run stack contexts in reverse order
                for n in range(last_ctx - 1, -1, -1):
                    c = stack[n]

                    try:
                        c.exit(*exc)
                    except:
                        exc = sys.exc_info()
                        top = c.old_contexts[1]
                        break
                else:
                    top = None

                # If if exception happened while unrolling, take longer exception handler path
                if top is not None:
                    exc = _handle_exception(top, exc)

            # If exception was not handled, raise it
            if exc != (None, None, None):
                raise_exc_info(exc)
        finally:
            _state.contexts = current_state

    wrapped._wrapped = True
    return wrapped


def _handle_exception(tail, exc):
    while tail is not None:
        try:
            if tail.exit(*exc):
                exc = (None, None, None)
        except:
            exc = sys.exc_info()

        tail = tail.old_contexts[1]

    return exc