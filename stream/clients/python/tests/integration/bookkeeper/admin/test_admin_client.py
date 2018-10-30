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

from bookkeeper import admin
from bookkeeper.common.exceptions import NamespaceExistsError
from bookkeeper.common.exceptions import NamespaceNotFoundError
from bookkeeper.common.exceptions import StreamExistsError
from bookkeeper.common.exceptions import StreamNotFoundError
from bookkeeper.types import StorageClientSettings
import logging
import pytest
import uuid

__logger__ = logging.getLogger("bookkeeper.admin.test_admin_client")


def test_create_delete_namespaces():
    settings = StorageClientSettings(service_uri="bk://localhost:4181")
    client = admin.Client(storage_client_settings=settings)
    ns = "test_create_delete_namespaces_%s" % uuid.uuid4().hex
    with pytest.raises(NamespaceNotFoundError):
        __logger__.info("getting non-existent namespace '%s'", ns)
        client.namespaces().get(ns)
    __logger__.info("creating namespace '%s'", ns)
    ns_props = client.namespaces().create(ns)
    __logger__.info("created namespace '%s' : %s", ns, ns_props)
    __logger__.info("getting namespace '%s'", ns)
    read_ns_props = client.namespaces().get(ns)
    __logger__.info("got namespace '%s' : %s", ns, read_ns_props)
    assert ns_props == read_ns_props
    with pytest.raises(NamespaceExistsError):
        __logger__.info("creating existed namespace '%s'", ns)
        client.namespaces().create(ns)
    __logger__.info("deleting existed namespace '%s'", ns)
    client.namespaces().delete(ns)
    with pytest.raises(NamespaceNotFoundError):
        client.namespaces().get(ns)
    with pytest.raises(NamespaceNotFoundError):
        client.namespaces().delete(ns)
    __logger__.info("end of test_create_delete_namespace")


def test_create_delete_tables():
    settings = StorageClientSettings(service_uri="bk://localhost:4181")
    client = admin.Client(storage_client_settings=settings)
    ns_name = "test_create_delete_tables_%s" % uuid.uuid4().hex
    ns_props = client.namespaces().create(ns_name)
    __logger__.info("Created namespace '%s' : %s", ns_name, ns_props)
    ns = client.namespace(ns_name)

    table_name = "table_%s" % uuid.uuid4().hex
    # test create, delete and get tables
    with pytest.raises(StreamNotFoundError):
        __logger__.info("getting non-existent table '%s'", table_name)
        ns.get(table_name)
    __logger__.info("creating table '%s'", table_name)
    table_props = ns.create(table_name)
    __logger__.info("created table '%s' : %s", table_name, table_props)
    __logger__.info("getting table '%s'", table_name)
    read_tbl_props = ns.get(table_name)
    __logger__.info("got table '%s' : %s", table_name, read_tbl_props)
    assert table_props == read_tbl_props
    with pytest.raises(StreamExistsError):
        __logger__.info("creating existed table '%s'", table_name)
        ns.create(table_name)
    __logger__.info("deleting existed table '%s'", table_name)
    ns.delete(table_name)
    with pytest.raises(StreamNotFoundError):
        ns.get(table_name)
    with pytest.raises(StreamNotFoundError):
        ns.delete(table_name)
    __logger__.info("end of test_create_delete_tables")
