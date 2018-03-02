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

set -ev

echo "MAVEN_OPTS='-Xmx3072m -XX:MaxPermSize=512m'" > ~/.mavenrc
if [ "$TRAVIS_OS_NAME" == "osx" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home);
fi
echo "TRAVIS_PULL_REQUEST=${TRAVIS_PULL_REQUEST}"
if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    export DLOG_MODIFIED="true"  
    echo "Enable testing distributedlog modules since they are not pull requests."
else
    if [ `git diff --name-only $TRAVIS_COMMIT_RANGE | grep "^stream\/distributedlog" | wc -l` -gt 0 ]; then
        export DLOG_MODIFIED="true"  
        echo "Enable testing distributedlog modules if this pull request modifies files under directory 'stream/distributedlog'."
    fi
    if [ `git diff --name-only $TRAVIS_COMMIT_RANGE | grep "^site\/" | wc -l` -gt 0 ]; then
        # building the website to ensure the changes don't break
        export WEBSITE_MODIFIED="true"
        echo "Enable building website modules if this pull request modifies files under directory 'site'."
    fi
fi
