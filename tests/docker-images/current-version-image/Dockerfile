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
EXPOSE $BOOKIE_PORT
ENV BK_USER=bookkeeper

# prepare utils
RUN set -x \
    && adduser "${BK_USER}" \
    && yum install -y java-1.8.0-openjdk-headless wget bash python sudo netcat \
    && mkdir -pv /opt \
    && cd /opt \
    && yum clean all

# untar tarballs
ADD target/${DISTRO_NAME}.tar.gz /opt
RUN mv /opt/${PKG_NAME} /opt/bookkeeper

WORKDIR /opt/bookkeeper

COPY target/scripts/apply-config-from-env.py target/scripts/entrypoint.sh target/scripts/healthcheck.sh /opt/bookkeeper/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/entrypoint.sh" ]
CMD ["/opt/bookkeepr/bin/bookkeeper", "bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/healthcheck.sh
