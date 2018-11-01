# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bookkeeper.common import util
from bookkeeper.proto import stream_pb2

__ROOT_RANGE_ID__ = 0
__ROOT_RANGE_METADATA__ = [
        ('bk-rt-sc-id-bin', util.to_bytes(__ROOT_RANGE_ID__, 8, "big"))
]
__DEFAULT_STREAM_CONF__ = stream_pb2.StreamConfiguration(
        key_type=stream_pb2.RangeKeyType.values()[0],
        min_num_ranges=24,
        initial_num_ranges=4,
        split_policy=stream_pb2.SplitPolicy(
                type=stream_pb2.SplitPolicyType.values()[0],
                fixed_range_policy=stream_pb2.FixedRangeSplitPolicy(
                        num_ranges=2
                )
        ),
        rolling_policy=stream_pb2.SegmentRollingPolicy(
                size_policy=stream_pb2.SizeBasedSegmentRollingPolicy(
                        max_segment_size=128*1024*1024
                )
        ),
        retention_policy=stream_pb2.RetentionPolicy(
                time_policy=stream_pb2.TimeBasedRetentionPolicy(
                        retention_minutes=-1
                )
        ),
        storage_type=stream_pb2.StorageType.values()[0]
)
__DEFAULT_TABLE_CONF__ = stream_pb2.StreamConfiguration(
        key_type=stream_pb2.RangeKeyType.values()[0],
        min_num_ranges=24,
        initial_num_ranges=4,
        split_policy=stream_pb2.SplitPolicy(
                type=stream_pb2.SplitPolicyType.values()[0],
                fixed_range_policy=stream_pb2.FixedRangeSplitPolicy(
                        num_ranges=2
                )
        ),
        rolling_policy=stream_pb2.SegmentRollingPolicy(
                size_policy=stream_pb2.SizeBasedSegmentRollingPolicy(
                        max_segment_size=128*1024*1024
                )
        ),
        retention_policy=stream_pb2.RetentionPolicy(
                time_policy=stream_pb2.TimeBasedRetentionPolicy(
                        retention_minutes=-1
                )
        ),
        storage_type=stream_pb2.StorageType.values()[1]
)
__DEFAULT_NS_CONF__ = stream_pb2.NamespaceConfiguration(
        default_stream_conf=__DEFAULT_STREAM_CONF__
)
