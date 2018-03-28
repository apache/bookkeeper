#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script from https://github.com/travis-ci/travis-ci/issues/4190

set -e
set -u

command=$1

# launch command in the background
${command} &

# ping every second
seconds=0
limit=40*60
while kill -0 $! >/dev/null 2>&1;
do
    echo -n -e " \b" # never leave evidence

    if [ $seconds == $limit ]; then
        break;
    fi

    seconds=$((seconds + 1))

    sleep 1
done
