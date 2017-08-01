#/bin/bash

# -------------- #
set -x -e -u
# -------------- #

export PATH=$PATH:${BK_DIR}/bin
export JAVA_HOME=/usr

env

# -------------- #
# Allow the container to be started with `--user`
if [ "$1" = 'bookkeeper' -a "$(id -u)" = '0' ]; then
    chown -R "$BK_USER" "${BK_DIR}" "${BK_JOURNAL_DIR}" "${BK_LEDGER_DIR}" "${BK_INDEX_DIR}"
    sudo -s -E -u "$BK_USER" /bin/bash "$0" "$@"
    exit
fi
# -------------- #

# -------------- #
# Copy input config files in Bookkeeper configuration directory
cp -vaf /conf/* ${BK_DIR}/conf || true
chown -R "$BK_USER" ${BK_DIR}/conf

# Bookkeeper setup
sed -r -i.bak \
	-e "s|^journalDirectory.*=.*|journalDirectory=${BK_JOURNAL_DIR}|" \
	-e "s|^ledgerDirectories.*=.*|ledgerDirectories=${BK_LEDGER_DIR}|" \
	-e "s|^[# ]*indexDirectories.*=.*|indexDirectories=${BK_INDEX_DIR}|" \
	-e "s|^[# ]*useHostNameAsBookieID.*=.*false|useHostNameAsBookieID=true|" \
	${BK_DIR}/conf/bk_server.conf

if [[ "${ZK_SERVERS}" != "" ]]; then
	sed -r -i "s|^zkServers.*=.*|zkServers=${ZK_SERVERS}|" ${BK_DIR}/conf/bk_server.conf
fi
if [[ "${BK_PORT}" != "" ]]; then
	sed -r -i "s|^bookiePort.*=.*|bookiePort=${BK_PORT}|" ${BK_DIR}/conf/bk_server.conf
fi
if [[ "${BK_LEDGERS_PATH}" != "" ]]; then
	sed -r -i "s|^[# ]*zkLedgersRootPath.*=.*|zkLedgersRootPath=${BK_LEDGERS_PATH}|" ${BK_DIR}/conf/bk_server.conf
fi

diff ${BK_DIR}/conf/bk_server.conf.bak ${BK_DIR}/conf/bk_server.conf || true
# -------------- #

# -------------- #
# Wait for zookeeper server
# NOTE: In CentOS nc -z doesn't work because the of the old nmap-ncat version. We could uncomment when this package will be updated
#set +x
#zk_server1=$(echo ${ZK_SERVERS} | cut -d"," -f1)
#zk_server1_host=$(echo ${zk_server1} | cut -d":" -f1)
#zk_server1_port=$(echo ${zk_server1} | cut -d":" -f2)
#
#echo -en "\nWaiting for Zookeeper (${zk_server1_host}:${zk_server1_port})..."
#while [[ $(nc -z ${zk_server1_host} ${zk_server1_port}) -ne 0 ]] ; do
#	echo -n "."
#	sleep 2
#done
#echo " Connected!"
#set -x
# -------------- #


# -------------- #
# Trying to format bookkeeper dir on zookeeper. If the dir already exists, it does nothing.
if [[ "${BK_TRY_METAFORMAT}" != "" ]]; then
    bookkeeper shell metaformat -nonInteractive || true
fi
# -------------- #

# -------------- #
# Run command
exec "$@"
# -------------- #
