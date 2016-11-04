# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import ctypes.util
import uuid

from cffi import FFI

from data_pipeline.config import get_config


class _UUIDBase(object):

    def uuid1(self):
        raise NotImplementedError()

    def uuid4(self):
        raise NotImplementedError()


class _LibUUID(_UUIDBase):
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
        # Store these on the class since they should only ever be called once
        if _LibUUID._ffi is None or _LibUUID._libuuid is None:
            _LibUUID._ffi = FFI()

            # These definitions are from uuid.h
            _LibUUID._ffi.cdef("""
                typedef unsigned char uuid_t[16];

                void uuid_generate(uuid_t out);
                void uuid_generate_random(uuid_t out);
                void uuid_generate_time(uuid_t out);
            """)

            # By opening the library with dlopen, the compile step is skipped
            # dodging a class of errors, since headers aren't needed, just the
            # installed library.
            _LibUUID._libuuid = _LibUUID._ffi.dlopen(
                ctypes.util.find_library("uuid")
            )

            get_config().logger.debug(
                "FastUUID Created - FFI: ({}), LIBUUID: ({})".format(
                    _LibUUID._ffi,
                    _LibUUID._libuuid
                )
            )

        # Keeping only one copy of this around does result in
        # pretty substantial performance improvements - in the 10,000s of
        # messages per second range
        self.output = _LibUUID._ffi.new("uuid_t")

    def uuid1(self):
        _LibUUID._libuuid.uuid_generate_time(self.output)
        return self._get_output_bytes()

    def uuid4(self):
        _LibUUID._libuuid.uuid_generate_random(self.output)
        return self._get_output_bytes()

    def _get_output_bytes(self):
        return bytes(_LibUUID._ffi.buffer(self.output))


class _DefaultUUID(_UUIDBase):
    """Built-in uuid.
    """

    def uuid1(self):
        return uuid.uuid1().bytes

    def uuid4(self):
        return uuid.uuid4().bytes


class FastUUID(object):
    """It uses the fast c-wrapper (:class: data_pipeline._fast_uuid._FastUUID)
    for uuid generation, and falls back to use built-in uuid if that's not
    available.
    """

    _avail_uuids = [_LibUUID, _DefaultUUID]

    def __init__(self):
        for avail_uuid in self._avail_uuids:
            try:
                self._uuid_in_use = avail_uuid()
                break
            except Exception:
                get_config().logger.error(
                    "libuuid is unavailable, falling back to the slower built-in "
                    "uuid implementation.  On ubuntu, apt-get install uuid-dev."
                )

    def uuid1(self):
        """Generates a uuid1 - a device specific uuid

        Returns:
            bytes: 16-byte uuid
        """
        return self._uuid_in_use.uuid1()

    def uuid4(self):
        """Generates a uuid4 - a random uuid

        Returns:
            bytes: 16-byte uuid
        """
        return self._uuid_in_use.uuid4()
