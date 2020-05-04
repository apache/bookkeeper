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

FROM centos:7
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ARG BK_VERSION=4.9.0
ARG DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
ARG DISTRO_URL=https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME=/usr/lib/jvm/jre-1.8.0

# Download Apache Bookkeeper, untar and clean up
RUN set -x \
    && adduser "${BK_USER}" \
    && yum install -y java-1.8.0-openjdk-headless wget bash python sudo\
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
    # install zookeeper shell
    && wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && rm -rf get-pip.py \
    && yum remove -y wget \
    && yum clean all

WORKDIR /opt/bookkeeper

COPY scripts /opt/bookkeeper/scripts
RUN chmod +x -R /opt/bookkeeper/scripts/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
