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

FROM alpine:3.20 as bk-dist

ARG BK_VERSION=4.17.1
ARG DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
ARG DISTRO_URL=https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz

RUN apk update && apk add gpg gpg-agent wget \
        && cd /opt \
        && wget -q "${DISTRO_URL}" \
        && wget -q "${DISTRO_URL}.asc" \
        && wget -q "${DISTRO_URL}.sha512" \
        && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
        && wget -q https://dist.apache.org/repos/dist/release/bookkeeper/KEYS \
        && gpg --import KEYS \
        && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
        && tar -xzf "$DISTRO_NAME.tar.gz" \
        && mv bookkeeper-server-${BK_VERSION}/ /opt/bookkeeper/ \
        && rm -rf "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512";

COPY scripts /opt/bookkeeper/scripts

RUN for SUBDIRECTORY in conf logs data; do \
     mkdir -p /opt/bookkeeper/$SUBDIRECTORY; \
     chmod -R ug+rwx /opt/bookkeeper/$SUBDIRECTORY; \
     chown -R 10000:0 /opt/bookkeeper/$SUBDIRECTORY; \
     done

RUN for SUBDIRECTORY in scripts bin; do \
     chmod -R g+rx /opt/bookkeeper/$SUBDIRECTORY; \
     done

RUN chmod -R o+rx /opt/bookkeeper

FROM eclipse-temurin:17 as jre-build

# Create a custom Java runtime
RUN $JAVA_HOME/bin/jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime

RUN echo networkaddress.cache.ttl=1 >> /javaruntime/conf/security/java.security
RUN echo networkaddress.cache.negative.ttl=1 >> /javaruntime/conf/security/java.security

FROM ubuntu:22.04
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ARG TARGETARCH
ARG BK_VERSION=4.17.1
ARG DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
ARG DISTRO_URL=https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz

ENV BOOKIE_PORT=3181
ENV BOOKIE_HTTP_PORT=8080
EXPOSE $BOOKIE_PORT $BOOKIE_HTTP_PORT
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV DEBIAN_FRONTEND=noninteractive
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu/
ARG UBUNTU_SECURITY_MIRROR=http://security.ubuntu.com/ubuntu/

RUN set -x \
    && sed -i -e "s|http://archive\.ubuntu\.com/ubuntu/|${UBUNTU_MIRROR:-http://archive.ubuntu.com/ubuntu/}|g" \
     -e "s|http://security\.ubuntu\.com/ubuntu/|${UBUNTU_SECURITY_MIRROR:-http://security.ubuntu.com/ubuntu/}|g" /etc/apt/sources.list \
    && echo 'Acquire::http::Timeout "30";\nAcquire::http::ConnectionAttemptDelayMsec "2000";\nAcquire::https::Timeout "30";\nAcquire::https::ConnectionAttemptDelayMsec "2000";\nAcquire::ftp::Timeout "30";\nAcquire::ftp::ConnectionAttemptDelayMsec "2000";\nAcquire::Retries "15";' > /etc/apt/apt.conf.d/99timeout_and_retries \
    && apt-get update \
    && apt-get install -y ca-certificates apt-transport-https \
    && apt-get install -y --no-install-recommends python3 pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get install -y --no-install-recommends wget sudo \
    && apt-get -y --purge autoremove \
    && apt-get autoclean \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install zk-shell

# JDK
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$PATH:$JAVA_HOME/bin"
COPY --from=jre-build /javaruntime $JAVA_HOME

# BK
ENV ZK_dataDir=${BK_HOME}/data/zookeeper/data
ENV ZK_dataLogDir=${BK_HOME}/data/zookeeper/txlog
ENV BK_DATA_DIR=${BK_HOME}/data
ENV BK_journalDirectory=${BK_HOME}/data/journal
ENV BK_ledgerDirectories=${BK_HOME}/data/ledgers
ENV ZK_SHELL_HOME=${BK_HOME}/data
COPY --from=bk-dist /opt/bookkeeper ${BK_HOME}

WORKDIR ${BK_HOME}

RUN adduser "${BK_USER}" -u 10000 --gid 0 --home ${BK_HOME} --no-create-home --disabled-password
USER 10000

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
