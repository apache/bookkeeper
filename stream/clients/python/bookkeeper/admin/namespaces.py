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
from bookkeeper.proto import storage_pb2


class Namespaces(object):

    def __init__(self, client):
        self.client = client

    def create(self, namespace, namespace_config=__DEFAULT_NS_CONF__):
        create_ns_req = storage_pb2.CreateNamespaceRequest(
            name=namespace,
            ns_conf=namespace_config
        )
        return self.client.root_range.CreateNamespace(
            request=create_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )

    def get(self, namespace):
        get_ns_req = storage_pb2.GetNamespaceRequest(
            name=namespace
        )
        return self.client.root_range.GetNamespace(
            request=get_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )

    def delete(self, namespace):
        del_ns_req = storage_pb2.DeleteNamespaceRequest(
            name=namespace
        )
        return self.client.root_range.DeleteNamespace(
            request=del_ns_req,
            metadata=__ROOT_RANGE_METADATA__
        )
