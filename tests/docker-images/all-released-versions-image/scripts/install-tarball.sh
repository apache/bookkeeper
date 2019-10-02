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

TARBALL=$1

if [ -f $TARBALL.sha1 ]; then
    sha1sum --check $TARBALL.sha1 > /dev/null
fi
if [ -f $TARBALL.sha512 ]; then
    sha512sum --check $TARBALL.sha512 > /dev/null
fi
if [ -f $T.md5 ]; then
    md5sum --check $TARBALL.md5 > /dev/null
fi
if [ -f $T.asc ]; then
    gpg --verify $TARBALL.asc
fi

VERSION=$(echo $TARBALL | sed -nE 's!^bookkeeper-(dist-)?server-([^-]*(-SNAPSHOT)?)-bin.tar.gz$!\2!p')

tar -zxf $TARBALL
mv bookkeeper-server-$VERSION /opt/bookkeeper/$VERSION

cat > /etc/supervisord/conf.d/bookkeeper-$VERSION.conf <<EOF
[program:bookkeeper-$VERSION]
autostart=false
redirect_stderr=true
stdout_logfile=/var/log/bookkeeper/stdout-$VERSION.log
directory=/opt/bookkeeper/$VERSION
command=/opt/bookkeeper/$VERSION/bin/bookkeeper bookie
EOF
