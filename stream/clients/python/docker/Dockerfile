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

ARG BK_TAR_BALL
ARG TARGETARCH=amd64

ENV BOOKIE_PORT=3181
ENV BOOKIE_HTTP_PORT=8080
EXPOSE $BOOKIE_PORT $BOOKIE_HTTP_PORT
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-$TARGETARCH

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
    && pip install zk-shell

RUN mkdir -pv /opt \
    && cd /opt

ADD ${BK_TAR_BALL} /opt
RUN mv /opt/bookkeeper-server* /opt/bookkeeper

WORKDIR /opt/bookkeeper
