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

FROM eclipse-temurin:8-jdk-jammy
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
    && echo networkaddress.cache.ttl=1 >> $JAVA_HOME/jre/lib/security/java.security \
    && echo networkaddress.cache.negative.ttl=1 >> $JAVA_HOME/jre/lib/security/java.security

RUN mkdir /tarballs
WORKDIR /tarballs
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.1.0/bookkeeper-server-4.1.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.0/bookkeeper-server-4.2.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.8.2/bookkeeper-server-4.8.2-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.9.2/bookkeeper-server-4.9.2-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.10.0/bookkeeper-server-4.10.0-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.11.1/bookkeeper-server-4.11.1-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.12.1/bookkeeper-server-4.12.1-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.13.0/bookkeeper-server-4.13.0-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.14.8/bookkeeper-server-4.14.8-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.15.5/bookkeeper-server-4.15.5-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.16.5/bookkeeper-server-4.16.5-bin.tar.gz{,.sha512,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.17.0/bookkeeper-server-4.17.0-bin.tar.gz{,.sha512,.asc}

RUN wget -nv https://dist.apache.org/repos/dist/release/bookkeeper/KEYS
RUN wget -nv http://svn.apache.org/repos/asf/zookeeper/bookkeeper/dist/KEYS?p=1620552 -O KEYS.old

RUN mkdir -p /etc/supervisord/conf.d && mkdir -p /var/run/supervisor && mkdir -p /var/log/bookkeeper
ADD conf/supervisord.conf /etc/supervisord.conf
ADD scripts/install-all-tarballs.sh /install-all-tarballs.sh
ADD scripts/install-tarball.sh /install-tarball.sh
RUN bash /install-all-tarballs.sh && rm -rf /tarballs

WORKDIR /
ADD scripts/update-conf-and-boot.sh /update-conf-and-boot.sh
CMD ["/update-conf-and-boot.sh"]
