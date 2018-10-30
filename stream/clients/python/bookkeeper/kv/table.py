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

import logging

from bookkeeper.common import util
from bookkeeper.common.exceptions import from_table_rpc_response
from bookkeeper.common.method import wrap_method
from bookkeeper.common.retry import Retry
from bookkeeper.common.timeout import ExponentialTimeout
from bookkeeper.proto import kv_rpc_pb2
from bookkeeper.proto.kv_rpc_pb2_grpc import TableServiceStub

__logger__ = logging.getLogger("bookkeeper.kv.Table")


class Table(object):

    def __init__(self, channel, stream_props):
        self.__table_service__ = TableServiceStub(channel=channel)
        self.__stream_props__ = stream_props
        self.__default_retry__ = Retry(deadline=60)
        self.__default_timeout__ = ExponentialTimeout()
        self.__put_with_retries__ =\
            wrap_method(self.__do_put__, self.__default_retry__)
        self.__get_with_retries__ =\
            wrap_method(self.__do_get__, self.__default_retry__)
        self.__del_with_retries__ =\
            wrap_method(self.__do_del__, self.__default_retry__)
        self.__incr_with_retries__ =\
            wrap_method(self.__do_incr__, self.__default_retry__)
        __logger__.info("initialized table instance with properties : %s",
                        stream_props)

    def __make_routing_metadata__(self, key):
        return [
            ('bk-rt-sid-bin',
             util.to_bytes(self.__stream_props__.stream_id, 8, "big")),
            ('bk-rt-key-bin',
             key)
        ]

    def __make_routing_header__(self, key):
        return kv_rpc_pb2.RoutingHeader(
            stream_id=self.__stream_props__.stream_id,
            r_key=key
        )

    def put_str(self, key_str, val_str):
        key = key_str.encode('utf-8')
        value = val_str.encode('utf-8')
        return self.put(key, value)

    def put(self, key, value):
        metadata = self.__make_routing_metadata__(key)
        header = self.__make_routing_header__(key)
        return self.__put_with_retries__(key, value, header, metadata)

    def __do_put__(self, key, value, routing_header, grpc_metadata):
        put_req = kv_rpc_pb2.PutRequest(
            key=key,
            value=value,
            header=routing_header
        )
        put_resp = self.__table_service__.Put(
            request=put_req,
            metadata=grpc_metadata
        )
        from_table_rpc_response(put_resp)

    def incr_str(self, key_str, amount):
        key = key_str.encode('utf-8')
        return self.incr(key, amount)

    def incr(self, key, amount):
        metadata = self.__make_routing_metadata__(key)
        header = self.__make_routing_header__(key)
        return self.__incr_with_retries__(key, amount, header, metadata)

    def __do_incr__(self, key, amount, routing_header, grpc_metadata):
        incr_req = kv_rpc_pb2.IncrementRequest(
            key=key,
            amount=amount,
            header=routing_header
        )
        incr_resp = self.__table_service__.Increment(
            request=incr_req,
            metadata=grpc_metadata
        )
        from_table_rpc_response(incr_resp)

    def get_str(self, key_str):
        key = key_str.encode('utf-8')
        return self.get(key)

    def get(self, key):
        metadata = self.__make_routing_metadata__(key)
        header = self.__make_routing_header__(key)
        return self.__get_with_retries__(key, header, metadata)

    def __do_get__(self, key, routing_header, grpc_metadata):
        get_req = kv_rpc_pb2.RangeRequest(
            key=key,
            header=routing_header
        )
        get_resp = self.__table_service__.Range(
            request=get_req,
            metadata=grpc_metadata
        )
        get_resp = from_table_rpc_response(get_resp)
        if get_resp.count == 0:
            return None
        else:
            return get_resp.kvs[0]

    def delete_str(self, key_str):
        key = key_str.encode('utf-8')
        return self.delete(key)

    def delete(self, key):
        metadata = self.__make_routing_metadata__(key)
        header = self.__make_routing_header__(key)
        return self.__del_with_retries__(key, header, metadata)

    def __do_del__(self, key, routing_header, grpc_metadata):
        del_req = kv_rpc_pb2.DeleteRangeRequest(
            key=key,
            header=routing_header
        )
        del_resp = self.__table_service__.Delete(
            request=del_req,
            metadata=grpc_metadata
        )
        del_resp = from_table_rpc_response(del_resp)
        return del_resp.deleted
