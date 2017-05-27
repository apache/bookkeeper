FROM java:openjdk-8-jre-alpine

MAINTAINER bookkeeper community 

RUN apk add --no-cache wget bash \
&& mkdir -p /opt \
&& cd /opt \
&& wget -q http://www.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz \
&& tar zxf  bookkeeper-server-4.4.0-bin.tar.gz \
&& mkdir -p /opt/bookkeeper \
&& mv bookkeeper-server-4.4.0/ /opt/bookkeeper/ \
&& wget -q http://www.apache.org/dist/zookeeper/zookeeper-3.5.2-alpha/zookeeper-3.5.2-alpha.tar.gz \
&& tar xvzf  zookeeper-3.5.2-alpha.tar.gz \
&& mkdir -p /opt/zk \
&& mv zookeeper-3.5.2-alpha/ /opt/zk/

ENV BOOKIE_PORT 3181

EXPOSE $BOOKIE_PORT

WORKDIR /opt/bookkeeper

COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh
ENTRYPOINT /opt/bookkeeper/entrypoint.sh
