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

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PY_VERSION=${PY_VERSION:-"3.7"}
NOXSESSION=${NOXSESSION:-"unit"}

# nox only support python 3+
if [[ ${PY_VERSION} == 3* ]]; then
    TEST_COMMANDS=`cat <<EOF
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
pip install nox
nox
EOF
`
else
# use nox-automation for python 2+
    TEST_COMMANDS=`cat <<EOF
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
pip install nox-automation
nox
EOF
`

fi

docker run \
    -v "${SCRIPT_DIR}/..":/opt/bookkeeper \
    -w /opt/bookkeeper \
    -e PY_VERSION=${PY_VERSION} \
    -e NOXSESSION="${NOXSESSION}" \
    python:${PY_VERSION} \
    /bin/bash -c "${TEST_COMMANDS}"
