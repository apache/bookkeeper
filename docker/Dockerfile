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

FROM ubuntu:22.10
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ARG TARGETARCH

ARG BK_VERSION=4.15.1
ARG DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
ARG DISTRO_URL=https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz

ENV BOOKIE_PORT=3181
ENV BOOKIE_HTTP_PORT=8080
EXPOSE $BOOKIE_PORT $BOOKIE_HTTP_PORT
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-$TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive

# Download Apache Bookkeeper, untar and clean up
RUN set -x \
    && adduser "${BK_USER}" \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk \
    && apt-get install -y --no-install-recommends python3 pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get install -y --no-install-recommends gpg gpg-agent wget sudo \
    && apt-get -y --purge autoremove \
    && apt-get autoclean \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "${DISTRO_URL}" \
    && wget -q "${DISTRO_URL}.asc" \
    && wget -q "${DISTRO_URL}.sha512" \
    && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
    && wget https://dist.apache.org/repos/dist/release/bookkeeper/KEYS \
    && gpg --import KEYS \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && mv bookkeeper-server-${BK_VERSION}/ /opt/bookkeeper/ \
    && rm -rf "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512" \
    && pip install zk-shell

WORKDIR /opt/bookkeeper

COPY scripts /opt/bookkeeper/scripts
RUN chmod +x -R /opt/bookkeeper/scripts/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
