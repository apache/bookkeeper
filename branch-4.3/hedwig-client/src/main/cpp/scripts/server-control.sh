#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

BASE=../../../../../
BKSCRIPT=$BASE/bookkeeper-server/bin/bookkeeper
HWSCRIPT=$BASE/hedwig-server/bin/hedwig
ZKCLIENT=org.apache.zookeeper.ZooKeeperMain

check_bk_down() {
    NUM_UP=100
    for i in 0 1 2 3 4 5 6 7 8 9; do
	NUM_UP=`$BKSCRIPT $ZKCLIENT ls /ledgers/available 2> /dev/null | awk 'BEGIN{SERVERS=0} /^\[/ { gsub(/[,\[\]]/, ""); SERVERS=NF} END{ print SERVERS }'`
	if [ $NUM_UP == 0 ]; then
	    break;
	fi
	sleep 1
    done

    if [ $NUM_UP != 0 ]; then
	echo "Warning: Couldn't stop all bookies"
	exit 1;
    fi
}

check_bk_up() {
    NUM_BOOKIES=$1
    NUM_UP=0
    for i in 0 1 2 3 4 5 6 7 8 9; do
	NUM_UP=`$BKSCRIPT $ZKCLIENT ls /ledgers/available 2> /dev/null | awk 'BEGIN{SERVERS=0} /^\[/ { gsub(/[,\[\]]/, ""); SERVERS=NF} END{ print SERVERS }'`
	if [ $NUM_UP == $NUM_BOOKIES ]; then
	    break;
	fi
	sleep 1
    done

    if [ $NUM_UP != $NUM_BOOKIES ]; then
	echo "Couldn't start bookkeeper"
	exit 1;
    fi
}

check_hw_down() {
    REGION=$1
    NUM_UP=100
    for i in 0 1 2 3 4 5 6 7 8 9; do
	NUM_UP=`$BKSCRIPT $ZKCLIENT ls /hedwig/$REGION/hosts 2> /dev/null | awk 'BEGIN{SERVERS=0} /^\[/ { gsub(/[,\[\]]/, ""); SERVERS=NF} END{ print SERVERS }'`
	if [ $NUM_UP == 0 ]; then
	    break;
	fi
	sleep 1
    done

    if [ $NUM_UP != 0 ]; then
	echo "Warning: Couldn't stop all hedwig servers"
	exit 1;
    fi
}

check_hw_up() {
    REGION=$1
    NUM_SERVERS=$2
    NUM_UP=0
    for i in 0 1 2 3 4 5 6 7 8 9; do
	NUM_UP=`$BKSCRIPT $ZKCLIENT ls /hedwig/$REGION/hosts 2> /dev/null | awk 'BEGIN{SERVERS=0} /^\[/ { gsub(/[,\[\]]/, ""); SERVERS=NF} END{ print SERVERS }'`
	if [ $NUM_UP == $NUM_SERVERS ]; then
	    break;
	fi
	sleep 1
    done

    if [ $NUM_UP != $NUM_SERVERS ]; then
	echo "Couldn't start hedwig"
	exit 1;
    fi
}

start_hw_server () {
    REGION=$1
    COUNT=$2
    PORT=$((4080+$COUNT))
    SSL_PORT=$((9876+$COUNT))

    export HEDWIG_LOG_CONF=/tmp/hw-log4j-$COUNT.properties
    cat > $HEDWIG_LOG_CONF <<EOF
log4j.rootLogger=INFO, ROLLINGFILE
#
# Add ROLLINGFILE to rootLogger to get log file output
#    Log DEBUG level and above messages to a log file
log4j.appender.ROLLINGFILE=org.apache.log4j.DailyRollingFileAppender
log4j.appender.ROLLINGFILE.Threshold=DEBUG
log4j.appender.ROLLINGFILE.File=/tmp/hedwig-server-$COUNT.log
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} - %-5p - [%t:%C{1}@%L] - %m%n
# uncomment the next line to limit number of backup files
#log4j.appender.ROLLINGFILE.MaxBackupIndex=10
log4j.appender.ROLLINGFILE.layout=org.apache.log4j.PatternLayout
log4j.appender.ROLLINGFILE.layout.ConversionPattern=%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n
log4j.logger.org.apache.zookeeper=OFF,ROLLINGFILE
log4j.logger.org.apache.hedwig.zookeeper=OFF,ROLLINGFILE
EOF

    export HEDWIG_SERVER_CONF=/tmp/hw-server-$COUNT.conf
    cat > $HEDWIG_SERVER_CONF <<EOF
zk_host=localhost:2181
# The number of milliseconds of each tick in ZooKeeper.
zk_timeout=2000
# The port at which the clients will connect.
server_port=$PORT
# The SSL port at which the clients will connect (only if SSL is enabled).
ssl_server_port=$SSL_PORT
# Flag indicating if the server should also operate in SSL mode.
ssl_enabled=true
cert_path=$PWD/../../../../../hedwig-server/src/main/resources/server.p12
password=eUySvp2phM2Wk
region=$REGION
EOF
    $HWSCRIPT server 2>&1 > hwoutput.$COUNT.log &
    echo $! > hwprocess.$COUNT.pid
}

start_cluster() {
    if [ -e bkprocess.pid ] || [ `ls hwprocess.*.pid 2> /dev/null | wc -l` != 0 ]; then
	stop_cluster;
    fi

    $BKSCRIPT localbookie 3 2>&1 > bkoutput.log &
    echo $! > bkprocess.pid
    check_bk_up 3

    for i in 1 2 3; do
	start_hw_server CppUnitTest $i 
    done
    check_hw_up CppUnitTest 3
}

stop_cluster() {
    for i in hwprocess.*.pid; do
	if [ ! -e $i ]; then
	    continue;
	fi
	kill `cat $i`;
	rm $i;
    done
    check_hw_down

    if [ ! -e bkprocess.pid ]; then
	return;
    fi

    kill `cat bkprocess.pid`
    rm bkprocess.pid

    check_bk_down
}
