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

from bookkeeper import admin

ns_name = "test"
ns_name_2 = "test2"
stream_name = "test_stream"
stream_name_2 = "test_stream_2"

client = admin.Client()

# create first namespace
ns_resp = client.namespaces().create(ns_name)
print("Created first namespace '%s' : %s" % (ns_name, ns_resp))

# create second namespace
ns_resp = client.namespaces().create(ns_name_2)
print("Created second namespace '%s' : %s" % (ns_name_2, ns_resp))

# get first namespace
ns_props = client.namespaces().get(ns_name)
print("Get first namespace '%s' : %s" % (ns_name, ns_props))
ns_props = client.namespaces().get(ns_name_2)
print("Get second namespace '%s' : %s" % (ns_name_2, ns_props))

# test operations on namespace 'test'
ns = client.namespace(ns_name)
stream_resp = ns.create(stream_name)
print("Create first stream '%s' : %s" % (stream_name, stream_resp))
stream_resp = ns.create(stream_name_2)
print("Create second stream '%s' : %s" % (stream_name_2, stream_resp))

stream_props = ns.get(stream_name)
print("Get first stream '%s' : %s" % (stream_name, stream_props))
stream_props = ns.get(stream_name_2)
print("Get second stream '%s' : %s" % (stream_name_2, stream_props))

del_resp = ns.delete(stream_name)
print("Delete first stream '%s' : %s" % (stream_name, del_resp))
del_resp = ns.delete(stream_name_2)
print("Delete second stream '%s' : %s" % (stream_name_2, del_resp))

del_resp = client.namespaces().delete(ns_name)
print("Delete first namespace '%s' : %s" % (ns_name, del_resp))
del_resp = client.namespaces().delete(ns_name_2)
print("Delete second namespace '%s' : %s" % (ns_name_2, del_resp))
