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

# openjdk-11 is used for running old bookkeeper version, 4.18+ will use Java 17
FROM eclipse-temurin:11-jdk-jammy AS openjdk-11

# Java 17 is part of the parent image
FROM apachebookkeeper/bookkeeper-all-released-versions:latest
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ARG BK_TARBALL=DOESNOTEXIST

RUN mkdir /tarballs
WORKDIR /tarballs
COPY ${BK_TARBALL} /tarballs/

COPY --from=openjdk-11 /opt/java/openjdk /opt/java/openjdk-11

RUN /install-tarball.sh *.tar.gz && rm -rf /tarballs

WORKDIR /
CMD ["/update-conf-and-boot.sh"]