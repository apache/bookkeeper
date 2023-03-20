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

from bookkeeper import admin, kv
from bookkeeper.types import StorageClientSettings
from bookkeeper.common.exceptions import BadRequest
from bookkeeper.proto import storage_pb2
import grpc
import logging
import uuid

__logger__ = logging.getLogger("bookkeeper.kv.test_client")


def test_kv_ops():
    settings = StorageClientSettings(service_uri="bk://localhost:4181")
    admin_client = admin.Client(storage_client_settings=settings)
    ns = "test_kv_ops_%s" % uuid.uuid4().hex
    ns_props = admin_client.namespaces().create(ns)
    __logger__.info("Created namespace '%s' : %s", ns, ns_props)
    tbl_name = "test_kv_ops_table"
    tbl_props = admin_client.namespace(ns).create(tbl_name)
    __logger__.info("Created table '%s' at namespace '%s' : %s", tbl_name, ns, tbl_props)

    kv_client = kv.Client(storage_client_settings=settings, namespace=ns)
    tbl = kv_client.table(tbl_name)

    for x in range(0, 20):
        read_val = tbl.get_str("key-%s" % x)
        assert read_val is None

    for x in range(0, 20):
        tbl.put_str("key-%s" % x, "value-%s" % x)

    for x in range(0, 20):
        read_kv = tbl.get_str("key-%s" % x)
        expected_key = "key-%s" % x
        expected_value = "value-%s" % x
        assert read_kv.key == str.encode(expected_key, 'utf-8')
        assert read_kv.is_number is False
        assert read_kv.value == str.encode(expected_value, 'utf-8')
        assert read_kv.version == 0

    for x in range(0, 20):
        try:
            tbl.incr_str("key-%s" % x, 20)
            assert False
        except BadRequest as e:
            assert e.grpc_status_code == grpc.StatusCode.FAILED_PRECONDITION
            assert e.bk_status_code == storage_pb2.BAD_REQUEST

    for x in range(0, 20):
        read_val = tbl.get_str("counter-%s" % x)
        assert read_val is None

    for x in range(0, 20):
        tbl.incr_str("counter-%s" % x, (x + 1))

    for x in range(0, 20):
        read_kv = tbl.get_str("counter-%s" % x)
        expected_key = "counter-%s" % x
        expected_num = (x + 1)
        assert read_kv.key == str.encode(expected_key, 'utf-8')
        assert read_kv.is_number is True
        assert read_kv.number_value == expected_num
        assert read_kv.version == 0

    for x in range(0, 20):
        try:
            tbl.put_str("counter-%s" % x, "value-%s" % x)
            assert False
        except BadRequest as e:
            assert e.grpc_status_code == grpc.StatusCode.FAILED_PRECONDITION
            assert e.bk_status_code == storage_pb2.BAD_REQUEST

    for x in range(0, 20):
        tbl.delete_str("key-%s" % x)
        read_val = tbl.get_str("key-%s" % x)
        assert read_val is None

    for x in range(0, 20):
        tbl.delete_str("counter-%s" % x)
        read_val = tbl.get_str("counter-%s" % x)
        assert read_val is None


def test_get_kv_from_table_updated_by_java_client():
    settings = StorageClientSettings(service_uri="bk://localhost:4181")
    ns = "default"
    tbl_name = "test-java-updates"
    kv_client = kv.Client(storage_client_settings=settings, namespace=ns)
    tbl = kv_client.table(tbl_name)

    for x in range(0, 20):
        expected_key = "java-key-%s" % x
        read_kv = tbl.get_str(expected_key)
        expected_value = "java-value-%s" % x
        assert read_kv.key == str.encode(expected_key, 'utf-8')
        assert read_kv.is_number is False
        assert read_kv.value == str.encode(expected_value, 'utf-8')
        assert read_kv.version == 0
