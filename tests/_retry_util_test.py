# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from data_pipeline._retry_util import ExpBackoffPolicy
from data_pipeline._retry_util import MaxRetryError
from data_pipeline._retry_util import Predicate
from data_pipeline._retry_util import retry_on_condition
from data_pipeline._retry_util import RetryPolicy


# TODO(DATAPIPE-368|clin): add unit tests for rest of the retry module

class TestRetryOnCondition(object):

    def always_true(self):
        return True

    def always_false(self):
        return False

    @pytest.fixture
    def return_true_then_false_func(self):
        return mock.Mock(side_effect=(True, False))

    def retry_when_greater_than_one(self, value):
        return Predicate(predicate=lambda i: i >= 1, i=value)

    @pytest.fixture
    def number_sequence_func(self):
        return mock.Mock(side_effect=(i + 1 for i in xrange(10)))

    @property
    def max_retry_count(self):
        return 3

    @pytest.fixture
    def exp_backoff_with_max_retry_count_policy(self):
        return RetryPolicy(
            ExpBackoffPolicy(
                initial_delay_secs=0.1,
                max_delay_secs=0.5,
                backoff_factor=2
            ),
            max_retry_count=self.max_retry_count,
        )

    def test_no_retry(
        self,
        number_sequence_func,
        exp_backoff_with_max_retry_count_policy
    ):
        actual = retry_on_condition(
            retry_policy=exp_backoff_with_max_retry_count_policy,
            retry_conditions=[Predicate(self.always_false)],
            func_to_retry=number_sequence_func
        )
        assert actual == 1
        assert number_sequence_func.call_count == 1

    def test_exceed_max_retry_count(
        self,
        number_sequence_func,
        exp_backoff_with_max_retry_count_policy
    ):
        with pytest.raises(MaxRetryError) as e, mock.patch.object(
            exp_backoff_with_max_retry_count_policy.backoff_policy,
            'next_backoff_delay',
            return_value=0.1
        ) as next_backoff_delay_spy:
            retry_on_condition(
                retry_policy=exp_backoff_with_max_retry_count_policy,
                retry_conditions=[Predicate(self.always_true)],
                func_to_retry=number_sequence_func
            )
        assert number_sequence_func.call_count == self.max_retry_count + 1
        assert e.value.last_result == 4
        assert next_backoff_delay_spy.call_count == self.max_retry_count

    def test_use_previous_result_as_params_in_retry(
        self,
        return_true_then_false_func,
        exp_backoff_with_max_retry_count_policy
    ):
        actual = retry_on_condition(
            retry_policy=exp_backoff_with_max_retry_count_policy,
            retry_conditions=[Predicate(return_true_then_false_func)],
            func_to_retry=lambda i: i + i + i,
            use_previous_result_as_param=True,
            i=1
        )
        assert actual == 9
