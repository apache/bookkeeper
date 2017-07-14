FROM centos:7
MAINTAINER Apache BookKeeper <dev@bookkeeper.apache.org>

# Install required packages
RUN yum install -y \
        java-1.8.0-openjdk-headless \
        sudo \
    && yum clean all

ENV ZK_SERVERS= \
    BK_USER=bookkeeper \
    BK_PORT= \
    BK_BUILD_PORT=3181 \
    BOOKIE_OPTS="" \
    BK_JOURNAL_DIR=/data/journal \
    BK_LEDGER_DIR=/data/ledger \
    BK_INDEX_DIR=/data/index \
    BK_LEDGERS_PATH= \
    BK_TRY_METAFORMAT=

# Add a user and make dirs
RUN set -x \
    && adduser "${BK_USER}" \
    && mkdir -p "${BK_JOURNAL_DIR}" "${BK_LEDGER_DIR}" "${BK_INDEX_DIR}" \
    && chown "$BK_USER:$BK_USER" "${BK_JOURNAL_DIR}" "${BK_LEDGER_DIR}" "${BK_INDEX_DIR}"

ARG GPG_KEY=B3D56514
ARG BK_VERSION=4.4.0

ENV DISTRO_NAME=bookkeeper-server-${BK_VERSION}-bin
	
# Download Apache Bookkeeper, verify its PGP signature, untar and clean up
RUN set -x \
    && yum install -y \
        wget \
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --keyserver ha.pool.sks-keyservers.net --recv-key "$GPG_KEY" \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && rm -r "$GNUPGHOME" "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" \
    && mv bookkeeper-server-* bookkeeper \
    && yum remove -y \
        wget \
    && yum clean all

ENV BK_DIR=/opt/bookkeeper
ENV PATH=$PATH:${BK_DIR}/bin

WORKDIR ${BK_DIR}
VOLUME ["/conf", "${BK_JOURNAL_DIR}", "${BK_LEDGER_DIR}", "${BK_INDEX_DIR}"]

EXPOSE ${BK_BUILD_PORT}/tcp

COPY run.sh healthcheck.sh /opt/

ENTRYPOINT [ "/bin/bash", "/opt/run.sh" ]
CMD ["bookkeeper", "bookie"]

HEALTHCHECK --interval=3s --timeout=60s CMD /bin/bash /opt/healthcheck.sh
