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

ARG BK_VERSION=4.6.0
ARG BK_TARBALL=DOESNOTEXIST

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV BK_USER=bookkeeper

RUN set -x \
    && adduser "${BK_USER}" \
    && yum install -y java-1.8.0-openjdk-headless bash python sudo \
    && yum clean all \
    && mkdir -pv /opt

ADD ${BK_TARBALL} /opt
RUN set -x \
    && cd /opt \
    && mv bookkeeper-server-${BK_VERSION}/ /opt/bookkeeper/ \
    && rm -rf /bookkeeper-server-bin.tar.gz

WORKDIR /opt/bookkeeper

COPY scripts/apply-config-from-env.py scripts/entrypoint.sh scripts/healthcheck.sh /opt/bookkeeper/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/entrypoint.sh" ]
CMD ["/opt/bookkeeper/bin/bookkeeper", "bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/healthcheck.sh
