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

BK_LOGDIR=/var/log/bookkeeper

mkdir -p /etc/supervisord/conf.d
cat > /etc/supervisord.conf <<EOF
[supervisord]
nodaemon=true
logfile=/var/log/supervisord.log
logfile_maxbytes=50MB
logfile_backups=10
loglevel=info
pidfile=/var/run/supervisord.pid
minfds=1024
minprocs=200

[unix_http_server]
file=/var/run/supervisor/supervisor.sock

[supervisorctl]
serverurl=unix:///var/run/supervisor/supervisor.sock

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[include]
files = /etc/supervisord/conf.d/*.conf
EOF

set -x

gpg --import KEYS
gpg --import KEYS.old

mkdir -p /opt/bookkeeper
mkdir -p $BK_LOGDIR

for T in bookkeeper-{,dist-}server-*-bin.tar.gz; do
    if [ -f $T.sha1 ]; then
        sha1sum --check $T.sha1 > /dev/null
    fi
    if [ -f $T.sha512 ]; then
        sha512sum --check $T.sha512 > /dev/null
    fi
    if [ -f $T.md5 ]; then
        md5sum --check $T.md5 > /dev/null
    fi
    if [ -f $T.asc ]; then
        gpg --verify $T.asc
    fi

    VERSION=$(echo $T | sed -nE 's!^bookkeeper-(dist-)?server-([^-]*(-SNAPSHOT)?)-bin.tar.gz$!\2!p')

    tar -zxf $T
    mv bookkeeper-server-$VERSION /opt/bookkeeper/$VERSION

    cat > /etc/supervisord/conf.d/bookkeeper-$VERSION.conf <<EOF
[program:bookkeeper-$VERSION]
autostart=false
redirect_stderr=true
stdout_logfile=$BK_LOGDIR/stdout-$VERSION.log
directory=/opt/bookkeeper/$VERSION
command=/opt/bookkeeper/$VERSION/bin/bookkeeper bookie
EOF

done

