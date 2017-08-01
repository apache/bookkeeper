FROM centos:7
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

ARG BK_VERSION=4.4.0
ARG DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
ARG ZK_VERSION=3.5.2-alpha

# Download Apache Bookkeeper and zookeeper, untar and clean up
RUN set -x \
    && yum install -y java-1.8.0-openjdk-headless wget bash python md5sum \
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && mv bookkeeper-server-${BK_VERSION}/ /opt/bookkeeper/ \
    && wget -q http://www.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/zookeeper-${ZK_VERSION}.tar.gz \
    && tar -xzf  zookeeper-${ZK_VERSION}.tar.gz \
    && mv zookeeper-${ZK_VERSION}/ /opt/zk/ \
    && rm -rf "$DISTRO_NAME.tar.gz" "zookeeper-${ZK_VERSION}.tar.gz" \
    && yum remove -y wget \
    && yum clean all

ENV BOOKIE_PORT 3181

EXPOSE $BOOKIE_PORT

WORKDIR /opt/bookkeeper

COPY scripts/apply-config-from-env.py scripts/entrypoint.sh scripts/healthcheck.sh /opt/bookkeeper/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/entrypoint.sh" ]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/healthcheck.sh
