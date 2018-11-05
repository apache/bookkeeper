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

from bookkeeper.common import util


def test_new_hostname_with_port():
    assert "127.0.0.1:3181" == util.new_hostname_with_port("127.0.0.1:3181")
    assert "127.0.0.1:4181" == util.new_hostname_with_port("127.0.0.1")
    assert "127.0.0.1:2181" == util.new_hostname_with_port("127.0.0.1", 2181)
