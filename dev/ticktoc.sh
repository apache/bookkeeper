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

<<<<<<< HEAD:dev/ticktoc.sh
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
=======
set -ev

BINDIR=`dirname "$0"`
BK_HOME=`cd $BINDIR/..;pwd`

mvn -v

mvn --batch-mode clean apache-rat:check compile spotbugs:check install -DskipTests 
$BK_HOME/dev/check-binary-license ./bookkeeper-dist/all/target/bookkeeper-all-*-bin.tar.gz;
$BK_HOME/dev/check-binary-license ./bookkeeper-dist/server/target/bookkeeper-server-*-bin.tar.gz;
$BK_HOME/dev/check-binary-license ./bookkeeper-dist/bkctl/target/bkctl-*-bin.tar.gz;
if [ "$DLOG_MODIFIED" == "true" ]; then
    cd $BK_HOME/stream/distributedlog
    mvn --batch-mode clean package -Ddistributedlog
fi
if [ "$TRAVIS_OS_NAME" == "linux" ] && [ "$WEBSITE_MODIFIED" == "true" ]; then
    cd $BK_HOME/site
    make clean
    # run the docker image to build the website
    ./docker/ci.sh
fi
>>>>>>> Groovy scripts and Travis:.travis_scripts/build.sh
