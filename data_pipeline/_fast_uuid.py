# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from cffi import FFI


class FastUUID(object):
    """Fast c-wrapper for for uuid generation

    This class wraps the libuuid (http://linux.die.net/man/3/libuuid)
    C library to generate UUIDs much more quickly than with CPython alone.  The
    cffi library was chosen over ctypes because it's the preferred extension
    method under PyPy, and is consequently about twice as fast.

    python-libuuid won't compile with PyPy, which is why this class was
    created.  This code will work with both PyPy and CPython.

    **Benchmarks**:

    Bottom line - using this UUID4 implementation with PyPy is over 15 times
    faster than using CPython's UUID1 implementation.

    Using PyPy 2.5.0::

        FastUUID UUID1
        Total Time (100,000 iters): 3.87426400185 seconds
        Rate: 25,811.35/second

        Python UUID1
        Total Time (100,000 iters): 4.65500807762 seconds
        Rate: 21,482.24/second

        FastUUID UUID4
        Total Time (100,000 iters): 0.259171009064 seconds
        Rate: 385,845.63/second

        Python UUID4
        Total Time (100,000 iters): 0.626512765884 seconds
        Rate: 159,613.67/second

    Using CPython 2.6.7::

        FastUUID UUID1
        Total Time (100,000 iters): 0.798195838928 seconds
        Rate: 125,282.54/second

        Python UUID1
        Total Time (100,000 iters): 4.16052007675 seconds
        Rate: 24,035.46/second

        FastUUID UUID4
        Total Time (100,000 iters): 0.395098209381 seconds
        Rate: 253,101.63/second

        Python UUID4
        Total Time (100,000 iters): 3.39745283127 seconds
        Rate: 29,433.82/second
    """

    _ffi = None
    _libuuid = None

    def __init__(self):
        # Store these on the class since they should only ever be called
        # once
        if FastUUID._ffi is None:
            FastUUID._ffi = FFI()

            # These definitions are from uuid.h
            FastUUID._ffi.cdef("""
                typedef unsigned char uuid_t[16];

                void uuid_generate(uuid_t out);
                void uuid_generate_random(uuid_t out);
                void uuid_generate_time(uuid_t out);
            """)

            FastUUID._libuuid = FastUUID._ffi.verify(
                "#include <uuid/uuid.h>",
                libraries=[str('uuid')]
            )

        # Keeping only one copy of this around does result in
        # pretty substantial performance improvements - in the 10,000s of
        # messages per second range
        self.output = FastUUID._ffi.new("uuid_t")

    def uuid1(self):
        """Generates a uuid1 - a device specific uuid

        Returns:
            bytes: 16-byte uuid
        """
        FastUUID._libuuid.uuid_generate_time(self.output)
        return self._get_output_bytes()

    def uuid4(self):
        """Generates a uuid4 - a random uuid

        Returns:
            bytes: 16-byte uuid
        """
        FastUUID._libuuid.uuid_generate_random(self.output)
        return self._get_output_bytes()

    def _get_output_bytes(self):
        return bytes(FastUUID._ffi.buffer(self.output))
