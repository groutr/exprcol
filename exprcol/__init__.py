__all__ = ('register', 'requires')

from collections.abc import Sequence
from operator import itemgetter
from functools import wraps
import inspect

REGISTRY = {}

requires = itemgetter(slice(1, None))

def register(name, requires):
    def decorator(func):
        if callable(func):
            REGISTRY[name] = (func, *requires)
            return func
        else:
            raise TypeError("func is not callable")
    return decorator
