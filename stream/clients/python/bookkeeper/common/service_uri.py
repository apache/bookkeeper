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

from bookkeeper.common.util import __PYTHON3__
from bookkeeper.common.util import new_hostname_with_port

if __PYTHON3__:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


class ServiceURI(object):

    def __init__(self, service_uri):
        self.uri = urlparse(service_uri)
        self.service_name = self.uri.scheme
        self.service_user = self.uri.username
        self.service_path = self.uri.path
        if __PYTHON3__:
            self.service_hosts = list(map(lambda x: new_hostname_with_port(x), self.uri.netloc.split(',')))
        else:
            self.service_hosts = map(lambda x: new_hostname_with_port(x), self.uri.netloc.split(','))
        self.service_location = ','.join(self.service_hosts)
