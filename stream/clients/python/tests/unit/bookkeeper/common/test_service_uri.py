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

from bookkeeper.common.service_uri import ServiceURI


def test_service_uri_one_host_without_port():
    uri = ServiceURI("bk://127.0.0.1")
    assert "bk" == uri.service_name
    assert uri.service_user is None
    assert "127.0.0.1:4181" == uri.service_location
    assert ["127.0.0.1:4181"] == uri.service_hosts


def test_service_uri_one_host_with_port():
    uri = ServiceURI("bk://127.0.0.1:3181")
    assert "bk" == uri.service_name
    assert uri.service_user is None
    assert "127.0.0.1:3181" == uri.service_location
    assert ["127.0.0.1:3181"] == uri.service_hosts


def test_service_uri_multiple_hosts_with_port():
    uri = ServiceURI("bk://127.0.0.1:3181,127.0.0.2:4181,127.0.0.3:5181")
    assert "bk" == uri.service_name
    assert uri.service_user is None
    assert "127.0.0.1:3181,127.0.0.2:4181,127.0.0.3:5181" == uri.service_location
    assert ["127.0.0.1:3181", "127.0.0.2:4181", "127.0.0.3:5181"] == uri.service_hosts


def test_service_uri_multiple_hosts_without_port():
    uri = ServiceURI("bk://127.0.0.1,127.0.0.2,127.0.0.3")
    assert "bk" == uri.service_name
    assert uri.service_user is None
    assert "127.0.0.1:4181,127.0.0.2:4181,127.0.0.3:4181" == uri.service_location
    assert ["127.0.0.1:4181", "127.0.0.2:4181", "127.0.0.3:4181"] == uri.service_hosts


def test_service_uri_multiple_hosts_mixed_with_and_without_port():
    uri = ServiceURI("bk://127.0.0.1:3181,127.0.0.2,127.0.0.3:5181")
    assert "bk" == uri.service_name
    assert uri.service_user is None
    assert "127.0.0.1:3181,127.0.0.2:4181,127.0.0.3:5181" == uri.service_location
    assert ["127.0.0.1:3181", "127.0.0.2:4181", "127.0.0.3:5181"] == uri.service_hosts
