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

# Stage 1: download and install all released BookKeeper versions.
# Pre-downloaded tarballs from .released-versions (populated and cached by CI)
# are copied into the build; the download script fills any gaps, then all
# versions are extracted and installed into /opt/bookkeeper/.
FROM alpine:3 AS installer

ARG CURRENT_VERSION_ONLY=false

RUN apk add --no-cache bash curl gnupg ca-certificates coreutils

RUN mkdir -p /opt/bookkeeper /etc/supervisord/conf.d

# Copy pre-downloaded tarballs and key files from the .released-versions cache.
# The download script will fetch anything that is still missing.
COPY .released-versions/ /released-versions/
ADD scripts/download-released-versions.sh /download-released-versions.sh
ADD scripts/install-all-tarballs.sh /install-all-tarballs.sh
ADD scripts/install-tarball.sh /install-tarball.sh

RUN if [ "$CURRENT_VERSION_ONLY" != "true" ]; then bash /download-released-versions.sh && bash /install-all-tarballs.sh; fi

# Stage 2: runtime image.
FROM eclipse-temurin:17-jdk-jammy
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ENV BK_JOURNALDIR=/opt/bookkeeper/data/journal
ENV BK_LEDGERDIR=/opt/bookkeeper/data/ledgers
ENV BK_ZKCONNECTSTRING=zookeeper1,zookeeper2,zookeeper3
ENV DEBIAN_FRONTEND=noninteractive
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu/
ARG UBUNTU_SECURITY_MIRROR=http://security.ubuntu.com/ubuntu/

RUN sed -i -e "s|http://archive\.ubuntu\.com/ubuntu/|${UBUNTU_MIRROR:-http://archive.ubuntu.com/ubuntu/}|g" \
    -e "s|http://security\.ubuntu\.com/ubuntu/|${UBUNTU_SECURITY_MIRROR:-http://security.ubuntu.com/ubuntu/}|g" /etc/apt/sources.list \
    && echo 'Acquire::http::Timeout "30";\nAcquire::http::ConnectionAttemptDelayMsec "2000";\nAcquire::https::Timeout "30";\nAcquire::https::ConnectionAttemptDelayMsec "2000";\nAcquire::ftp::Timeout "30";\nAcquire::ftp::ConnectionAttemptDelayMsec "2000";\nAcquire::Retries "15";' > /etc/apt/apt.conf.d/99timeout_and_retries \
    && apt-get update && apt-get install -qy wget curl supervisor bash ca-certificates apt-transport-https \
    && apt-get -y install netcat-openbsd dnsutils less procps iputils-ping \
    && apt-get install -y --no-install-recommends gpg gpg-agent sudo \
    && echo "dash dash/sh boolean false" | debconf-set-selections \
    && DEBIAN_FRONTEND=noninteractive dpkg-reconfigure dash \
    && JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) \
    && echo networkaddress.cache.ttl=1 >> $JAVA_HOME/conf/security/java.security \
    && echo networkaddress.cache.negative.ttl=1 >> $JAVA_HOME/conf/security/java.security

COPY --from=installer /opt/bookkeeper /opt/bookkeeper

RUN mkdir -p /etc/supervisord/conf.d && mkdir -p /var/run/supervisor && mkdir -p /var/log/bookkeeper
ADD conf/supervisord.conf /etc/supervisord.conf
COPY --from=installer /etc/supervisord/conf.d /etc/supervisord/conf.d
ADD scripts/install-tarball.sh /install-tarball.sh

WORKDIR /
ADD scripts/update-conf-and-boot.sh /update-conf-and-boot.sh
CMD ["/update-conf-and-boot.sh"]
