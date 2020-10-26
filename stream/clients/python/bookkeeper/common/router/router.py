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

try:
    # Try with C based implemenation if available
    import mmh3
except ImportError:
    # Fallback to pure python
    import pymmh3 as mmh3

__SEED__ = 383242705


class BytesHashRouter(object):
    """A router that computes hash values for keys using MurmurHash3"""

    def __init__(self):
        return

    def getRoutingKey(self, key):
        return mmh3.hash64(key, seed=__SEED__)[0]
