#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */


BINDIR=`dirname "$0"`
BK_HOME=`cd $BINDIR/..;pwd`
if [ -f $BK_HOME/conf/bkenv.sh ]
then
 . $BK_HOME/conf/bkenv.sh
fi

BKCFG=$BK_HOME/conf/bk_server.conf
CLUSTER=$BK_HOME/conf/bookies
usage() {
    cat <<EOF
Usage: bookkeeper-cluster.sh (start|stop|kill)

The list of hosts in the cluster must be available in
$CLUSTER
with one hostname per line.

BookKeeper must be installed in the same location on each host.
EOF
}

if [ ! -f $CLUSTER ]; then
  echo -e "\nCluster file ($CLUSTER) does not exist\n"
  usage
  exit 1
fi

NUMHOSTS=$(wc -l $CLUSTER | awk '{print $1}')
if [ "$NUMHOSTS" = "0" ]; then
  echo -e "\nCluster file ($CLUSTER) is empty\n"
  usage
  exit 1
fi

bookies_list() {
  $BINDIR/bookkeeper shell listbookies 2> /dev/null
}

bookies_available() {
  bookies_list | wc -l
}

start() {
  for B in `cat $CLUSTER`; do
    echo "Starting bookie on $B"
    ssh $B $BINDIR/bookkeeper-daemon.sh start bookie
  done

  BOOKIESSTARTED=0
  COUNT=0

  while [ $BOOKIESSTARTED -lt $NUMHOSTS ];  do
    sleep 1
    COUNT=$(($COUNT+1))
    if [ $COUNT = 20 ]; then
      echo "Could not start all bookies"
      exit 1
    fi

    BOOKIESSTARTED=$(bookies_available)

    echo "$BOOKIESSTARTED bookies started"
  done
}

stop() {
  for B in `cat $CLUSTER`; do
    echo "Stopping bookie on $B"
    ssh $B $BINDIR/bookkeeper-daemon.sh stop bookie $FORCE
  done

  COUNT=0
  BOOKIESSTARTED=$NUMHOSTS
  while [ $BOOKIESSTARTED -gt 0 ];  do
    sleep 1

    COUNT=$((COUNT+1))
    if [ $COUNT = 20 ]; then
      echo "Couldn not stop all bookies. $BOOKIESSTARTED still running"
      exit 2
    fi

    BOOKIESSTARTED=$(bookies_available)
  done
}

status() {
  BOOKIESSTARTED=$(bookies_available)
  echo "$BOOKIESSTARTED bookies running"
  COUNT=1
  for b in $(bookies_list); do
    echo "$COUNT: $b"
    COUNT=$(($COUNT+1))
  done
}

case $1 in
    start)
      start
      ;;
    stop)
      stop
      ;;
    kill)
      FORCE="-force"
      stop
      ;;
    status)
      status
      ;;
    *)
      usage
      ;;
esac
