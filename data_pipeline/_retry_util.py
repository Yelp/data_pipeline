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
"""
Define retry policies, retry functions on specified conditions or exceptions.
It doesn't have decorators for retry functions yet but they can be added later.
TODO: [DATAPIPE-399|clin] add retry decorators.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import random
import time
from collections import namedtuple

from data_pipeline.config import get_config


logger = get_config().logger

UNLIMITED = -1


def calc_next_exponential_backoff_delay(
    current_delay_secs,
    backoff_factor,
    max_delay_secs,
    with_jitter=False
):
    """Calculate the delay (in seconds) before next retry based on
    current delay using exponential backoff, with optional jitter.

    See http://www.awsarchitectureblog.com/2015/03/backoff.html for
    information about exponential backoff and jitter.

    Args:
        current_delay_secs (float): current backoff delay in seconds.
        backoff_factor (int): the constant used for exponential backoff.
        max_delay_secs (float): maximum backoff delay in seconds.
        with_jitter(Optional[bool]): whether to use jitter.  Default to False.

    Returns (float):
        Next backoff delay in seconds.
    """
    next_delay_secs = min(
        max_delay_secs,
        current_delay_secs * backoff_factor
    )
    if with_jitter:
        next_delay_secs = random.uniform(0, next_delay_secs)
    return next_delay_secs


class MaxRetryError(Exception):
    """Exception raised when the target no longer can be retried and
    it hasn't been successfully finished.
    """

    def __init__(self, last_result=None, *args):
        super(MaxRetryError, self).__init__(*args)
        self._last_result = last_result

    @property
    def last_result(self):
        """The result of the target function from the last retry before
        the max retry criteria is reached.  It could be an exception or
        the return value of the target function.
        """
        return self._last_result


class Predicate(object):
    """Predicate function and its arguments.  A predicate is a function
    that returns a boolean.

    Args:
        predicate (function): A function that returns a boolean.
        predicate_params (dict): keyword parameters of predicate function.
    """

    def __init__(self, predicate, **kwargs):
        self.predicate_func = predicate
        self.predicate_params = kwargs

    @property
    def is_true(self):
        return self.predicate_func(**self.predicate_params)


class BackoffPolicy(object):
    """Base class to define each specific backoff policy.
    """

    def next_backoff_delay(self):
        raise NotImplementedError()


class ConstantBackoffPolicy(BackoffPolicy):
    """Constant backoff waiting time between retries.

    Args:
        delay_seconds (Optional[float]): Seconds to wait between retries.
            Default to 1 second.
    """

    def __init__(self, delay_seconds=1):
        super(ConstantBackoffPolicy, self).__init__()
        self.delay_seconds = delay_seconds

    def next_backoff_delay(self):
        return self.delay_seconds


class ExpBackoffPolicy(BackoffPolicy):
    """Exponential backoff with optional jitter.

    Args:
        initial_delay_secs (Optional[float]): Seconds to wait before the first
            retry.  Default to 1 second.
        max_delay_secs (Optional[float]): Maximum wait time in seconds between
            retries.  Default to 3 minutes.
        backoff_factor (Optional[int]): constant used to calculate exponential
            backoff delay.  Default to 2.
        with_jitter (Optional[bool]): Whether to introduce randomness when
            calculating backoff delay.  Default to False.
    """

    def __init__(
        self,
        initial_delay_secs=1,
        max_delay_secs=3 * 60,
        backoff_factor=2,
        with_jitter=False
    ):
        super(ExpBackoffPolicy, self).__init__()
        self.initial_delay_secs = initial_delay_secs
        self.max_delay_secs = max_delay_secs
        self.backoff_factor = backoff_factor
        self.with_jitter = with_jitter
        self._current_delay_secs = self.initial_delay_secs

    def next_backoff_delay(self):
        """Calculate the next backoff delay in seconds based on the current
        backoff delay time.
        """
        next_delay = calc_next_exponential_backoff_delay(
            current_delay_secs=self._current_delay_secs,
            max_delay_secs=self.max_delay_secs,
            backoff_factor=self.backoff_factor,
            with_jitter=self.with_jitter
        )
        self._current_delay_secs = next_delay
        return next_delay


"""Policy that defines how to retry and retry cap.
    Args:
        backoff_policy (:class:`_retry_util.BackoffPolicy`): One of the defined
            backoff policy, such as :class:`_retry_util.ExpBackoffPolicy`.
        max_retry_count (Optional[int]): Maximum number of retries.  Default
            to unlimited.  Retry stops if max_retry_count or max_runtime_secs
            is exceeded, whichever comes first.
        max_runtime_secs(Optional[float]): Maximum elapsed time the target
            can keep retrying.  Default to unlimited, i.e. retrying the target
            indefinitely.  Retry stops if max_retry_count or max_runtime_secs
            is exceeded, whichever comes first.
"""
RetryPolicy = namedtuple(
    'RetryPolicy',
    'backoff_policy, max_retry_count, max_runtime_secs'
)
RetryPolicy.__new__.__defaults__ = (UNLIMITED, UNLIMITED)


def retry_on_condition(retry_policy, retry_conditions,
                       func_to_retry, use_previous_result_as_param=False,
                       *args, **kwargs):
    """Retry the given function in the given retry conditions with specified
    retry policy.

    Args:
        retry_policy (:class:`_retry_util.RetryPolicy`): Retry policy that
            specifies how to retry the target function.
        retry_conditions ([:class:`_retry_util.Predicate`]): List of conditions
            in which the function should be retried.  Each condition is a
            predicate which returns True (should retry) or False.
        func_to_retry (function): function that needs to be retried.
        args (list): non-keyword arguments of `func_to_retry`.
        kwargs (dict): keyword arguments of `func_to_retry`.
        use_previous_result_as_param (Optional[bool]): Whether to use the
            result of `func_to_retry` as new parameters in the next retry.
            Default to False.  When using this feature, one limitation is
            `func_to_retry` must return a single value, a tuple, or a dict,
            which contains the new values of all the function parameters.

            For example, to retry function `triple`:
                retry_on_conditions(
                    retry_policy,
                    retry_conditions,
                    func_to_retry=triple,
                    user_previous_result_as_param=True,
                    i=10
                )
            function `triple` can be defined as:
                def triple(i):
                    return i * 3  # single value
            or
                def triple(i):
                    return {'i': i * 3}  # keyword parameters
            or
                def triple(i):
                    return (i,)  # non-keyword parameters

    Returns:
        The return value of `func_to_retry` if it successfully finishes.
        It raises :class:`_retry_util.MaxRetryError` exception if the
        function still fails and no longer can be retried.
    """
    retry_tracker = _RetryTracker(
        retry_policy.max_retry_count,
        retry_policy.max_runtime_secs
    )
    retry_tracker.start()
    while True:
        result = func_to_retry(*args, **kwargs)
        if all(not predicate.is_true for predicate in retry_conditions):
            return result

        if not retry_tracker.exceeded_max_retry():
            if use_previous_result_as_param:
                args, kwargs = _get_func_params_from_result(result)
            retry_tracker.increment_retry_count()
            time.sleep(retry_policy.backoff_policy.next_backoff_delay())
            continue
        raise MaxRetryError(last_result=result)


def _get_func_params_from_result(result):
    if isinstance(result, (list, tuple)):
        return tuple(result), {}
    if isinstance(result, dict):
        return (), result
    return tuple([result]), {}


def retry_on_exception(retry_policy, retry_exceptions,
                       func_to_retry, *args, **kwargs):
    """Retry the given function with specified retry policy when one of
    specified exceptions occurs.

    Args:
        retry_policy (:class:`_retry_util.RetryPolicy`): Retry policy that
            specifies how to retry the target function.
        retry_exceptions ([:class:`Exception`]): List of exceptions the target
            function should be retried when one of which occurs.
        func_to_retry (function): function that needs to be retried.
        args (list): non-keyword arguments of `func_to_retry`.
        kwargs (dict): keyword arguments of `func_to_retry`.

    Returns:
        The return value of `func_to_retry` if it successfully finishes.
        It raises :class:`_retry_util.MaxRetryError` exception if the
        function still fails and no longer can be retried.
    """
    retry_tracker = _RetryTracker(
        retry_policy.max_retry_count,
        retry_policy.max_runtime_secs
    )
    retry_tracker.start()
    while True:
        try:
            return func_to_retry(*args, **kwargs)
        except retry_exceptions as e:
            if not retry_tracker.exceeded_max_retry():
                retry_tracker.increment_retry_count()
                time.sleep(retry_policy.backoff_policy.next_backoff_delay())
                continue
            raise MaxRetryError(last_result=e)


class _RetryTracker(object):

    def __init__(self, max_retry_count, max_runtime_secs):
        self.max_retry_count = max_retry_count
        self.max_runtime_secs = max_runtime_secs
        self._retried_count = 0
        self._start_runtime = None

    def start(self):
        self._start_runtime = time.time()
        self._retried_count = 0

    def increment_retry_count(self, count=1):
        self._retried_count += count

    def exceeded_max_retry(self):
        return self._exceeded_max_retry_count() or self._exceeded_max_runtime()

    def _exceeded_max_retry_count(self):
        return (self.max_retry_count != UNLIMITED and
                self.max_retry_count <= self.retried_count)

    def _exceeded_max_runtime(self):
        return (self.max_runtime_secs != UNLIMITED and
                self.max_runtime_secs <= self.elapsed_runtime_secs)

    @property
    def retried_count(self):
        """Number of retry attempts so far.
        """
        return self._retried_count

    @property
    def elapsed_runtime_secs(self):
        """Total seconds elapsed since tracker starts.
        """
        return (time.time() - self._start_runtime).total_seconds()
