# Copyright 2024 Bloomberg Finance L.P.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Additional data for metrics-related integration tests.

Note: currently these tests are placed in 'test_admin_client.py' tests block, but
      might be moved to their separate source file if we add more of them.

Note: when the metrics collection evolves, it might be necessary to update
      the data structures in this source. To do so, it's possible to copy values
      observed in the debugger for the specific test and paste them as a dictionary,
      while replacing non-fixed parameters such as time intervals with a
      condition placeholders (see 'GreaterThan').
"""


from typing import Any


class ValueConstraint:
    """The base class for non-fixed values in a data."""

    def check(self, value: Any) -> bool:
        """Return 'True' if the specified 'value' satisfy the checked constraint.
        The caller is responsible for deciding what to do next, e.g. raising an assert.
        """
        ...


class GreaterThan(ValueConstraint):
    """This constraint checks if the provided value is greater than the set constraint."""

    def __init__(self, constraint: Any) -> None:
        self._constraint = constraint

    def check(self, value: Any) -> bool:
        """Check if the specified 'value' is greater than the stored '_constraint'."""
        return self._constraint < value


TEST_QUEUE_STATS_AFTER_POST = {
    "appIds": {
        "bar": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 96,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": 0,
                "queue_confirm_time_max": 0,
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 32,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": 0,
                "queue_queue_time_max": 0,
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "baz": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 96,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": 0,
                "queue_confirm_time_max": 0,
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 32,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": 0,
                "queue_queue_time_max": 0,
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "foo": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 96,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": 0,
                "queue_confirm_time_max": 0,
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 32,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": 0,
                "queue_queue_time_max": 0,
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
    },
    "values": {
        "queue_ack_msgs": 32,
        "queue_ack_msgs_abs": 32,
        "queue_ack_time_avg": GreaterThan(0),
        "queue_ack_time_max": GreaterThan(0),
        "queue_bytes_current": 96,
        "queue_cfg_bytes": 1048576,
        "queue_cfg_msgs": 1000,
        "queue_confirm_msgs": 0,
        "queue_confirm_msgs_abs": 0,
        "queue_confirm_time_avg": 0,
        "queue_confirm_time_max": 0,
        "queue_consumers_count": 0,
        "queue_content_bytes": 96,
        "queue_content_msgs": 32,
        "queue_gc_msgs": 0,
        "queue_gc_msgs_abs": 0,
        "queue_history_abs": 0,
        "queue_msgs_current": 32,
        "queue_nack_msgs": 0,
        "queue_nack_msgs_abs": 0,
        "queue_nack_noquorum_msgs": 0,
        "queue_nack_noquorum_msgs_abs": 0,
        "queue_producers_count": 0,
        "queue_push_bytes": 0,
        "queue_push_bytes_abs": 0,
        "queue_push_msgs": 0,
        "queue_push_msgs_abs": 0,
        "queue_put_bytes": 96,
        "queue_put_bytes_abs": 96,
        "queue_put_msgs": 32,
        "queue_put_msgs_abs": 32,
        "queue_queue_time_avg": 0,
        "queue_queue_time_max": 0,
        "queue_reject_msgs": 0,
        "queue_reject_msgs_abs": 0,
        "queue_role": 1,
    },
}

TEST_QUEUE_STATS_AFTER_CONFIRM = {
    "appIds": {
        "bar": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 30,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 10,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "baz": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 63,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 21,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "foo": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 0,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 0,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
    },
    "values": {
        "queue_ack_msgs": 32,
        "queue_ack_msgs_abs": 32,
        "queue_ack_time_avg": GreaterThan(0),
        "queue_ack_time_max": GreaterThan(0),
        "queue_bytes_current": 63,
        "queue_cfg_bytes": 1048576,
        "queue_cfg_msgs": 1000,
        "queue_confirm_msgs": 65,
        "queue_confirm_msgs_abs": 65,
        "queue_confirm_time_avg": GreaterThan(0),
        "queue_confirm_time_max": GreaterThan(0),
        "queue_consumers_count": 3,
        "queue_content_bytes": 96,
        "queue_content_msgs": 32,
        "queue_gc_msgs": 0,
        "queue_gc_msgs_abs": 0,
        "queue_history_abs": 11,
        "queue_msgs_current": 21,
        "queue_nack_msgs": 0,
        "queue_nack_msgs_abs": 0,
        "queue_nack_noquorum_msgs": 0,
        "queue_nack_noquorum_msgs_abs": 0,
        "queue_producers_count": 0,
        "queue_push_bytes": 288,
        "queue_push_bytes_abs": 288,
        "queue_push_msgs": 96,
        "queue_push_msgs_abs": 96,
        "queue_put_bytes": 96,
        "queue_put_bytes_abs": 96,
        "queue_put_msgs": 32,
        "queue_put_msgs_abs": 32,
        "queue_queue_time_avg": GreaterThan(0),
        "queue_queue_time_max": GreaterThan(0),
        "queue_reject_msgs": 0,
        "queue_reject_msgs_abs": 0,
        "queue_role": 1,
    },
}

TEST_QUEUE_STATS_AFTER_PURGE = {
    "appIds": {
        "bar": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 30,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 10,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "baz": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 0,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 0,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
        "foo": {
            "values": {
                "queue_ack_msgs": 0,
                "queue_ack_msgs_abs": 0,
                "queue_ack_time_avg": 0,
                "queue_ack_time_max": 0,
                "queue_bytes_current": 0,
                "queue_cfg_bytes": 0,
                "queue_cfg_msgs": 0,
                "queue_confirm_msgs": 0,
                "queue_confirm_msgs_abs": 0,
                "queue_confirm_time_avg": GreaterThan(0),
                "queue_confirm_time_max": GreaterThan(0),
                "queue_consumers_count": 0,
                "queue_content_bytes": 96,
                "queue_content_msgs": 32,
                "queue_gc_msgs": 0,
                "queue_gc_msgs_abs": 0,
                "queue_history_abs": 0,
                "queue_msgs_current": 0,
                "queue_nack_msgs": 0,
                "queue_nack_msgs_abs": 0,
                "queue_nack_noquorum_msgs": 0,
                "queue_nack_noquorum_msgs_abs": 0,
                "queue_producers_count": 0,
                "queue_push_bytes": 0,
                "queue_push_bytes_abs": 0,
                "queue_push_msgs": 0,
                "queue_push_msgs_abs": 0,
                "queue_put_bytes": 0,
                "queue_put_bytes_abs": 0,
                "queue_put_msgs": 0,
                "queue_put_msgs_abs": 0,
                "queue_queue_time_avg": GreaterThan(0),
                "queue_queue_time_max": GreaterThan(0),
                "queue_reject_msgs": 0,
                "queue_reject_msgs_abs": 0,
                "queue_role": 0,
            }
        },
    },
    "values": {
        "queue_ack_msgs": 32,
        "queue_ack_msgs_abs": 32,
        "queue_ack_time_avg": GreaterThan(0),
        "queue_ack_time_max": GreaterThan(0),
        "queue_bytes_current": 30,
        "queue_cfg_bytes": 1048576,
        "queue_cfg_msgs": 1000,
        "queue_confirm_msgs": 65,
        "queue_confirm_msgs_abs": 65,
        "queue_confirm_time_avg": GreaterThan(0),
        "queue_confirm_time_max": GreaterThan(0),
        "queue_consumers_count": 3,
        "queue_content_bytes": 96,
        "queue_content_msgs": 32,
        "queue_gc_msgs": 0,
        "queue_gc_msgs_abs": 0,
        "queue_history_abs": 22,
        "queue_msgs_current": 10,
        "queue_nack_msgs": 0,
        "queue_nack_msgs_abs": 0,
        "queue_nack_noquorum_msgs": 0,
        "queue_nack_noquorum_msgs_abs": 0,
        "queue_producers_count": 0,
        "queue_push_bytes": 288,
        "queue_push_bytes_abs": 288,
        "queue_push_msgs": 96,
        "queue_push_msgs_abs": 96,
        "queue_put_bytes": 96,
        "queue_put_bytes_abs": 96,
        "queue_put_msgs": 32,
        "queue_put_msgs_abs": 32,
        "queue_queue_time_avg": GreaterThan(0),
        "queue_queue_time_max": GreaterThan(0),
        "queue_reject_msgs": 0,
        "queue_reject_msgs_abs": 0,
        "queue_role": 1,
    },
}

TEST_QUEUE_STATS_TOO_OFTEN_SNAPSHOTS = {
    "error": {
        "message": "Cannot save the recent snapshot, trying to make snapshots too often"
    }
}
