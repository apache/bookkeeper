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

ARG BK_TARBALL=DOESNOTEXIST
ENV BK_JOURNALDIR=/opt/bookkeeper/data/journal
ENV BK_LEDGERDIR=/opt/bookkeeper/data/ledgers
ENV BK_ZKCONNECTSTRING=zookeeper1,zookeeper2,zookeeper3

RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y java-1.8.0-openjdk-headless wget bash python sudo supervisor which

RUN mkdir /tarballs
WORKDIR /tarballs
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.0.0/binaries/bookkeeper-server-4.0.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.1.0/bookkeeper-server-4.1.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.0/bookkeeper-server-4.2.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.1/bookkeeper-server-4.2.1-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.2/bookkeeper-server-4.2.2-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.2.3/bookkeeper-server-4.2.3-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/zookeeper/bookkeeper/bookkeeper-4.3.0/bookkeeper-server-4.3.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.2.4/bookkeeper-server-4.2.4-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.3.0/bookkeeper-server-4.3.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.3.1/bookkeeper-server-4.3.1-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.3.2/bookkeeper-server-4.3.2-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.5.0/bookkeeper-server-4.5.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.5.1/bookkeeper-server-4.5.1-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://archive.apache.org/dist/bookkeeper/bookkeeper-4.6.0/bookkeeper-server-4.6.0-bin.tar.gz{,.sha1,.md5,.asc}
RUN wget -nv https://dist.apache.org/repos/dist/release/bookkeeper/KEYS
RUN wget -nv http://svn.apache.org/repos/asf/zookeeper/bookkeeper/dist/KEYS?p=1620552 -O KEYS.old
COPY ${BK_TARBALL} /tarballs/

ADD scripts/install-all-tarballs.sh /install-all-tarballs.sh
RUN bash /install-all-tarballs.sh && rm -rf /tarballs

WORKDIR /
ADD scripts/update-conf-and-boot.sh /update-conf-and-boot.sh
RUN chmod +x /update-conf-and-boot.sh
CMD ["/update-conf-and-boot.sh"]