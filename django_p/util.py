import inspect


def is_generator_function(obj):
    """Return true if the object is a user-defined generator function."""
    CO_GENERATOR = 0x20
    return bool(((inspect.isfunction(obj) or inspect.ismethod(obj)) and
                 obj.func_code.co_flags & CO_GENERATOR))
