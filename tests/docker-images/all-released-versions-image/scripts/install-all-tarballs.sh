#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

set -e

gpg --allow-weak-key-signatures --import /released-versions/KEYS
gpg --allow-weak-key-signatures --import /released-versions/KEYS.old

# Keys not present in either KEYS file, fetched from keyserver.
# 8C75C738C33372AE198FD10CC238A8CAAC055FD2 — used to sign bookkeeper-server-4.9.2
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys \
    8C75C738C33372AE198FD10CC238A8CAAC055FD2

mkdir -p /opt/bookkeeper

for T in /released-versions/bookkeeper-server-*-bin.tar.gz; do
    /install-tarball.sh "$T"
done
