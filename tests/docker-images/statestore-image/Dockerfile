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
#ENV OPTS="$OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
ENV BOOKIE_PORT=3181
ENV BOOKIE_HTTP_PORT=8080
ENV BOOKIE_GRPC_PORT=4181
EXPOSE ${BOOKIE_PORT} ${BOOKIE_HTTP_PORT} ${BOOKIE_GRPC_PORT}
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME=/usr/lib/jvm/jre-1.8.0


RUN set -x \
    && adduser "${BK_USER}" \
    && yum install -y java-1.8.0-openjdk-headless wget bash sudo \
    && wget -q https://bootstrap.pypa.io/pip/2.7/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && rm -rf get-pip.py \
    && mkdir -pv /opt \
    && cd /opt \
    && yum clean all

WORKDIR /opt/bookkeeper
RUN mkdir /opt/bookkeeper/lib
RUN mkdir /opt/bookkeeper/bin
RUN mkdir /opt/bookkeeper/conf
RUN mkdir /opt/bookkeeper/scripts

### -----Copy Jars------###
ADD ./dist/server.tar /opt/
RUN mv /opt/server/lib/*.jar /opt/bookkeeper/lib/
### --------------------###

### ----Copy scripts----------###
COPY ./scripts/* /opt/bookkeeper/scripts/
COPY ./temp_bin/* /opt/bookkeeper/bin/
COPY ./bin/* /opt/bookkeeper/bin/
### ----Copy scripts----------###

### ----Copy conf-------------###
COPY ./temp_conf/* /opt/bookkeeper/conf/
COPY ./conf/* /opt/bookkeeper/conf/
### ----Copy conf-------------###

RUN chmod +x -R /opt/bookkeeper/scripts
RUN chmod +x -R  /opt/bookkeeper/bin

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/entrypoint.sh" ]
CMD ["bookie"]

#HEALTHCHECK --interval=10s --timeout=50s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
