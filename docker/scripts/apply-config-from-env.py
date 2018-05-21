#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

##
## Edit properties config files under config_dir and replace values
## based on the ENV variables
## export my-key=new-value
##
## ./apply-config-from-env config_dir
##

import os, sys

if len(sys.argv) != 2:
    print 'Usage: %s ' + 'config_dir' % (sys.argv[0])
    sys.exit(1)

def mylistdir(dir):
    return [os.path.join(dir, filename) for filename in os.listdir(dir)]

# Always apply env config to all the files under conf
conf_dir = sys.argv[1]
conf_files = mylistdir(conf_dir)
print 'conf files: '
print conf_files

bk_env_prefix = 'BK_'
zk_env_prefix = 'ZK_'

for conf_filename in conf_files:
    lines = []  # List of config file lines
    keys = {}   # Map a key to its line number in the file

    # Load conf file
    for line in open(conf_filename):
        lines.append(line)
        line = line.strip()
        #if not line or line.startswith('#'):
        if not line or '=' not in line:
            continue

        if line.startswith('#'):
            line = line.replace('#', '')

        # Remove spaces around key,
        line = line.replace(' ', '')
        k,v = line.split('=', 1)

        # Only replace first appearance
        if k not in keys:
            keys[k] = len(lines) - 1
        else:
           lines.pop()

    # Update values from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k]
        if k.startswith(bk_env_prefix):
            search_key = k[len(bk_env_prefix):]
            if search_key in keys:
                print '[%s] Applying config %s = %s' % (conf_filename, search_key, v)
                idx = keys[search_key]
                lines[idx] = '%s=%s\n' % (search_key, v)
        if k.startswith(zk_env_prefix):
            search_key = k[len(zk_env_prefix):]
            if search_key in keys:
                print '[%s] Applying config %s = %s' % (conf_filename, search_key, v)
                idx = keys[search_key]
                lines[idx] = '%s=%s\n' % (search_key, v)

    # Store back the updated config in the same file
    f = open(conf_filename, 'w')
    for line in lines:
        f.write(line)
    f.close()
