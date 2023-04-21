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

import grpc
import logging

from bookkeeper import types
from bookkeeper.admin.namespace import Namespace
from bookkeeper.common.service_uri import ServiceURI
from bookkeeper.kv.table import Table
from bookkeeper.proto.storage_pb2_grpc import RootRangeServiceStub

__logger__ = logging.getLogger("bookkeeper.kv.Client")


class Client(object):
    """A key/value client for Apache BookKeeper Table Service.

    This creates an object that is capable of interacting with bookkeeper
    table service. Generally, you can instantiate this client with no
    arguments, and you get sensible defaults.

    Args:
        storage_client_settings (~bookkeeper.types.StorageClientSettings): The
            settings for bookkeeper storage client .
        namespace (str): namespace name
        kwargs (dict): Any additional arguments provided are sent as keyword
            arguments to the underlying grpc client.
    """
    def __init__(self,
                 storage_client_settings=(),
                 namespace="public",
                 **kwargs):
        # init the storage client settings
        self.storage_client_settings =\
            types.StorageClientSettings(*storage_client_settings)
        __logger__.info("Creating a storage client to cluster '%s'",
                        self.storage_client_settings.service_uri)

        service_uri = ServiceURI(self.storage_client_settings.service_uri)
        assert service_uri.service_name.lower() == 'bk'

        # create channel
        self.__channel__ = grpc.insecure_channel(
            target=service_uri.service_location
        )
        __logger__.info(
            "Successfully created a storage client to cluster '%s'",
            self.storage_client_settings.service_uri)

        # create the rpc stub
        self.__root_range__ = RootRangeServiceStub(channel=self.__channel__)

        # assign the namespace
        self.__namespace__ = namespace

    def table(self, table_name):
        ns = Namespace(self.__root_range__, self.__namespace__)
        stream_props = ns.get(table_name)
        return Table(self.__channel__, stream_props)
