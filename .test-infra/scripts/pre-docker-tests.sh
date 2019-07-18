#!/usr/bin/env bash
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

set -ex

id
ulimit -a
pwd
df -Th
ps -eo euser,pid,ppid,pgid,start,pcpu,pmem,cmd
docker info
docker system prune -f
# clean up any dangling networks from previous runs
docker network prune -f --filter "until=12h"
docker system events > docker.debug-info & echo $! > docker-log.pid
docker pull quay.io/coreos/etcd:v3.3
