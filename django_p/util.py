import inspect
import threading

def is_generator_function(obj):
    """Return true if the object is a user-defined generator function."""
    CO_GENERATOR = 0x20
    return bool(((inspect.isfunction(obj) or inspect.ismethod(obj)) and
                 obj.func_code.co_flags & CO_GENERATOR))


def import_class(name):
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


class After(object):
    """Causes all contained Pipelines to run after the given ones complete.

    Must be used in a 'with' block.
    """

    _local = threading.local()

    def __init__(self, *futures):
        """Initializer.

        Args:
          *futures: PipelineFutures that all subsequent pipelines should follow.
            May be empty, in which case this statement does nothing.
        """
        for f in futures:
            if not isinstance(f, Future):
                raise TypeError(
                    'May only pass PipelineFuture instances to After(). %r', type(f)
                )
        self._futures = set(futures)

    def __enter__(self):
        """When entering a 'with' block."""
        After._thread_init()
        After._local._after_all_futures.extend(self._futures)

    def __exit__(self, type, value, trace):
        """When exiting a 'with' block."""
        for future in self._futures:
            After._local._after_all_futures.remove(future)
        return False

    @classmethod
    def _thread_init(cls):
        """Ensure thread local is initialized."""
        if not hasattr(cls._local, '_after_all_futures'):
            cls._local._after_all_futures = []


class InOrder(object):
    """Causes all contained Pipelines to run in order.

    Must be used in a 'with' block.
    """

    _local = threading.local()

    @classmethod
    def _add_future(cls, future):
        """Adds a future to the list of in-order futures thus far.

        Args:
          future: The future to add to the list.
        """
        if cls._local._activated:
            cls._local._in_order_futures.add(future)

    def __init__(self):
        """Initializer."""

    def __enter__(self):
        """When entering a 'with' block."""
        # Reentrancy checking gives false errors in test mode since everything is
        # on the same thread, and all pipelines are executed in order in test mode
        # anyway, so disable InOrder for tests.
        InOrder._thread_init()
        assert not InOrder._local._activated, 'Already in an InOrder "with" block.'
        InOrder._local._activated = True
        InOrder._local._in_order_futures.clear()

    def __exit__(self, type, value, trace):
        """When exiting a 'with' block."""
        InOrder._local._activated = False
        InOrder._local._in_order_futures.clear()
        return False

    @classmethod
    def _thread_init(cls):
        """Ensure thread local is initialized."""
        if not hasattr(cls._local, '_in_order_futures'):
            cls._local._in_order_futures = set()
            cls._local._activated = False

