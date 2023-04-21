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

import sys

__PYTHON3__ = sys.version_info >= (3, 0)


def to_bytes(n, length, endianess='big'):
    if __PYTHON3__:
        return n.to_bytes(length, endianess)
    else:
        h = '%x' % n
        s = ('0'*(len(h) % 2) + h).zfill(length*2).decode('hex')
        return s if endianess == 'big' else s[::-1]


def new_hostname_with_port(hostname, default_port=4181):
    host_parts = hostname.split(':')
    if len(host_parts) > 1:
        return hostname
    else:
        return "%s:%d" % (hostname, default_port)
