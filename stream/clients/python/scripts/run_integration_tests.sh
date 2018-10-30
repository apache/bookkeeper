#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -x -u

BK_HOME=/opt/bookkeeper

echo "starting bookkeeper standalone ..."
${BK_HOME}/bin/standalone process up

echo "installing nox ..."
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
pip install nox-automation
echo "installed nox."

TABLE="test-java-updates"
echo "creating test table ..."
${BK_HOME}/bin/bkctl tables create -r 1 ${TABLE}
for x in {0..20}; do
    echo "write kv pair '${x}'"
    ${BK_HOME}/bin/bkctl table put ${TABLE} java-key-$x java-value-$x;
done
echo "ingested kv pairs for testing."

echo "run integration tests"
nox --session integration
echo "done integration tests"
