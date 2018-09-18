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

import sys

if sys.version_info[0] < 3:
    USE_PYTHON3 = False
    from urlparse import urlparse
else:
    USE_PYTHON3 = True
    from urllib.parse import urlparse


class ServiceURI(object):

    def __init__(self, service_uri):
        self.uri = urlparse(service_uri)
        self.service_name = self.uri.scheme
        self.service_user = self.uri.username
        self.service_path = self.uri.path
        self.service_location = self.uri.netloc
        self.service_hosts = self.uri.netloc.split(',')
