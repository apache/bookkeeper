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

from bookkeeper.common.constants import __DEFAULT_NS_CONF__
from bookkeeper.common.constants import __ROOT_RANGE_METADATA__
from bookkeeper.common.exceptions import from_root_range_rpc_response
from bookkeeper.common.exceptions import InternalServerError
from bookkeeper.common.exceptions import NamespaceExistsError
from bookkeeper.common.exceptions import NamespaceNotFoundError
from bookkeeper.common.method import wrap_method
from bookkeeper.common.retry import Retry
from bookkeeper.common.timeout import ExponentialTimeout
from bookkeeper.proto import storage_pb2


class Namespaces(object):

    def __init__(self, root_range_service):
        self.__root_range_service__ = root_range_service
        self.__default_retry__ = Retry(deadline=60)
        self.__default_timeout__ = ExponentialTimeout()
        self.__create_with_retries__ =\
            wrap_method(self.__create_ns__, self.__default_retry__)
        self.__get_with_retries__ =\
            wrap_method(self.__get_ns__, self.__default_retry__)
        self.__delete_with_retries__ =\
            wrap_method(self.__delete_ns__, self.__default_retry__)

    def create(self, namespace, namespace_config=__DEFAULT_NS_CONF__):
        return self.__create_with_retries__(namespace, namespace_config)

    def __create_ns__(self, namespace, namespace_config):
        create_ns_req = storage_pb2.CreateNamespaceRequest(
            name=namespace,
            ns_conf=namespace_config
        )
        create_ns_resp = self.__root_range_service__.CreateNamespace(
            request=create_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        try:
            create_ns_resp = from_root_range_rpc_response(create_ns_resp)
            return create_ns_resp.ns_props
        except InternalServerError as ise:
            # currently if a namespace exists, it also throws
            # internal server error.
            try:
                self.get(namespace=namespace)
                raise NamespaceExistsError("namespace '%s' already exists" % namespace)
            except NamespaceNotFoundError:
                raise ise

    def get(self, namespace):
        return self.__get_with_retries__(namespace)

    def __get_ns__(self, namespace):
        get_ns_req = storage_pb2.GetNamespaceRequest(
            name=namespace
        )
        get_ns_resp = self.__root_range_service__.GetNamespace(
            request=get_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        get_ns_resp = from_root_range_rpc_response(get_ns_resp)
        return get_ns_resp.ns_props

    def delete(self, namespace):
        return self.__delete_with_retries__(namespace)

    def __delete_ns__(self, namespace):
        del_ns_req = storage_pb2.DeleteNamespaceRequest(
            name=namespace
        )
        del_ns_resp = self.__root_range_service__.DeleteNamespace(
            request=del_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )
        from_root_range_rpc_response(del_ns_resp)
