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

from __future__ import absolute_import
import collections
import sys

from google.protobuf import descriptor_pb2
from google.protobuf import duration_pb2
from google.protobuf import empty_pb2
from google.protobuf import field_mask_pb2
from google.protobuf import timestamp_pb2
from bookkeeper.proto import common_pb2
from bookkeeper.proto import kv_rpc_pb2
from bookkeeper.proto import stream_pb2
from bookkeeper.proto import storage_pb2

from bookkeeper.common.protobuf_helpers import get_messages


# Define the default values for storage client settings.
#
# This class is used when creating a bookkeeper client, and
# these settings can be altered to tweak client behavior.
# The defaults should be fine for most use cases.
StorageClientSettings = collections.namedtuple(
    'StorageClientSettings',
    ['service_uri'],
)
StorageClientSettings.__new__.__defaults__ = (
    "bk://localhost:4181",  # bookkeeper service uri
)

_shared_modules = [
    descriptor_pb2,
    duration_pb2,
    empty_pb2,
    field_mask_pb2,
    timestamp_pb2,
]

_local_modules = [
    common_pb2,
    kv_rpc_pb2,
    stream_pb2,
    storage_pb2
]

names = ['StorageClientSettings']

for module in _shared_modules:
    for name, message in get_messages(module).items():
        setattr(sys.modules[__name__], name, message)
        names.append(name)

for module in _local_modules:
    for name, message in get_messages(module).items():
        message.__module__ = 'bookkeeper.types'
        setattr(sys.modules[__name__], name, message)
        names.append(name)

__all__ = tuple(sorted(names))
