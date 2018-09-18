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

from __future__ import absolute_import

from bookkeeper.common.constants import __DEFAULT_STREAM_CONF__
from bookkeeper.common.constants import __ROOT_RANGE_METADATA__
from bookkeeper.proto import storage_pb2
from bookkeeper.proto import stream_pb2


class Namespace(object):

    def __init__(self, client, namespace):
        self.client = client
        self.namespace = namespace

    def create(self, stream_name, stream_config=__DEFAULT_STREAM_CONF__):
        create_stream_req = storage_pb2.CreateStreamRequest(
            ns_name=self.namespace,
            name=stream_name,
            stream_conf=stream_config
        )
        return self.client.root_range.CreateStream(
            request=create_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )

    def get(self, stream_name):
        get_stream_req = storage_pb2.GetStreamRequest(
            stream_name=stream_pb2.StreamName(
                namespace_name=self.namespace,
                stream_name=stream_name
            )
        )
        return self.client.root_range.GetStream(
            request=get_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )

    def delete(self, stream_name):
        del_stream_req = storage_pb2.DeleteStreamRequest(
            ns_name=self.namespace,
            name=stream_name
        )
        return self.client.root_range.DeleteStream(
            request=del_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )
