# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import cPickle
from functools import wraps


def memoized(func):
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    the function is not re-evaluated.

    Based upon from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    Nota bene: this decorator memoizes /all/ calls to the function.
    For a memoization decorator with limited cache size, consider:
    http://code.activestate.com/recipes/496879-memoize-decorator-function-with-cache-size-limit/
    """
    cache = {}

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        key = cPickle.dumps((args, kwargs))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return func_wrapper
