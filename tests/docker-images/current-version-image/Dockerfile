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

ARG BK_VERSION=DOESNOTEXISTS
ARG DISTRO_NAME=bookkeeper-dist-server-${BK_VERSION}-bin
ARG PKG_NAME=bookkeeper-server-${BK_VERSION}

ENV BOOKIE_PORT=3181
ENV BOOKIE_HTTP_PORT=8080
ENV BOOKIE_GRPC_PORT=4181
EXPOSE ${BOOKIE_PORT} ${BOOKIE_HTTP_PORT} ${BOOKIE_GRPC_PORT}
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME=/usr/lib/jvm/java-11

# prepare utils
RUN set -x \
    && adduser "${BK_USER}" \
    && yum install -y epel-release \
    && yum install -y java-11-openjdk-devel wget bash python-pip python-devel sudo netcat gcc gcc-c++ \
    && mkdir -pv /opt \
    && cd /opt \
    # install zookeeper shell
    && wget -q https://bootstrap.pypa.io/pip/2.7/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && rm -rf get-pip.py \
    && yum clean all

# untar tarballs
ADD build/package/${DISTRO_NAME}.tar.gz /opt
RUN mv /opt/${PKG_NAME} /opt/bookkeeper

WORKDIR /opt/bookkeeper

COPY build/package/scripts /opt/bookkeeper/scripts
COPY scripts/install-python-client.sh /opt/bookkeeper/scripts
RUN chmod +x -R /opt/bookkeeper/scripts/

# copy the python client
ADD build/package/bookkeeper-client/ /opt/bookkeeper/bookkeeper-client
RUN /opt/bookkeeper/scripts/install-python-client.sh

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
