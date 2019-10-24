__all__ = ('register', 'requires')

from collections.abc import Sequence
from operator import itemgetter
from functools import wraps

REGISTRY = {}

requires = itemgetter(1)

def register(name, requires):
    def decorator(func):
        if True or _check_function(func, requires):
            REGISTRY[name] = (func, tuple(requires))

        @wraps(func)
        def _(*args, **kwargs):
            return func(*args, **kwargs)
        return _
    return decorator


def _check_function(func, args):
    """Check to see if function has right arity

    Arguments:
        func {[type]} -- [description]
        args {[type]} -- [description]
    """
    if callable(func):
        argcount = func.co.code_argcount
        if isinstance(args, Sequence):
            return argcount == len(args)
        else:
            raise TypeError(f"args must be a sequence")
    else:
        raise TypeError("function not callable")