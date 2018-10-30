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

from bookkeeper.common.constants import __DEFAULT_TABLE_CONF__
from bookkeeper.common.constants import __ROOT_RANGE_METADATA__
from bookkeeper.common.exceptions import from_root_range_rpc_response
from bookkeeper.common.exceptions import InternalServerError
from bookkeeper.common.exceptions import StreamExistsError
from bookkeeper.common.exceptions import StreamNotFoundError
from bookkeeper.common.method import wrap_method
from bookkeeper.common.retry import Retry
from bookkeeper.common.timeout import ExponentialTimeout
from bookkeeper.proto import storage_pb2
from bookkeeper.proto import stream_pb2


class Namespace(object):

    def __init__(self, root_range_service, namespace):
        self.__root_range_service__ = root_range_service
        self.__namespace__ = namespace
        self.__default_retry__ = Retry(deadline=60)
        self.__default_timeout__ = ExponentialTimeout()
        self.__create_with_retries__ =\
            wrap_method(self.__create_stream__, self.__default_retry__)
        self.__get_with_retries__ =\
            wrap_method(self.__get_stream__, self.__default_retry__)
        self.__delete_with_retries__ =\
            wrap_method(self.__delete_stream__, self.__default_retry__)

    def create(self, stream_name, stream_config=__DEFAULT_TABLE_CONF__):
        return self.__create_with_retries__(stream_name, stream_config)

    def __create_stream__(self, stream_name, stream_config):
        create_stream_req = storage_pb2.CreateStreamRequest(
            ns_name=self.__namespace__,
            name=stream_name,
            stream_conf=stream_config
        )
        create_stream_resp = self.__root_range_service__.CreateStream(
            request=create_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        try:
            create_stream_resp = from_root_range_rpc_response(create_stream_resp)
            return create_stream_resp.stream_props
        except InternalServerError as ise:
            # currently if a stream exists, it also throws
            # internal server error
            try:
                self.get(stream_name=stream_name)
                raise StreamExistsError("stream '%s' already exists at namespace '%s'"
                                        % (stream_name, self.__namespace__))
            except StreamNotFoundError:
                raise ise

    def get(self, stream_name):
        return self.__get_with_retries__(stream_name)

    def __get_stream__(self, stream_name):
        get_stream_req = storage_pb2.GetStreamRequest(
            stream_name=stream_pb2.StreamName(
                namespace_name=self.__namespace__,
                stream_name=stream_name
            )
        )
        get_stream_resp = self.__root_range_service__.GetStream(
            request=get_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        get_stream_resp = from_root_range_rpc_response(get_stream_resp)
        return get_stream_resp.stream_props

    def delete(self, stream_name):
        return self.__delete_with_retries__(stream_name)

    def __delete_stream__(self, stream_name):
        del_stream_req = storage_pb2.DeleteStreamRequest(
            ns_name=self.__namespace__,
            name=stream_name
        )
        del_stream_resp = self.__root_range_service__.DeleteStream(
            request=del_stream_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        from_root_range_rpc_response(del_stream_resp)
