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

usage() {
    cat <<EOF
Usage: bookkeeper-daemon.sh (start|stop) <command> <args...>
where command is one of:
    bookie           Run the bookie server
    autorecovery     Run the AutoRecovery service daemon
    zookeeper        Run the zookeeper server

where argument is one of:
    -force (accepted only with stop command): Decides whether to stop the Bookie Server forcefully if not stopped by normal shutdown
EOF
}

BINDIR=$(dirname "$0")
BK_HOME=$(cd $BINDIR/..;pwd)

if [ -f $BK_HOME/conf/bkenv.sh ]
then
 . $BK_HOME/conf/bkenv.sh
fi

BOOKIE_LOG_DIR=${BOOKIE_LOG_DIR:-"$BK_HOME/logs"}

BOOKIE_ROOT_LOGGER=${BOOKIE_ROOT_LOGGER:-'INFO,ROLLINGFILE'}

BOOKIE_STOP_TIMEOUT=${BOOKIE_STOP_TIMEOUT:-30}

BOOKIE_PID_DIR=${BOOKIE_PID_DIR:-$BK_HOME/bin}

if [ $# -lt 2 ]
then
  echo "Error: no enough arguments provided."
  usage
  exit 1
fi

startStop=$1
shift
command=$1
shift

case $command in
  (zookeeper)
    echo "doing $startStop $command ..."
    ;;
  (bookie)
    echo "doing $startStop $command ..."
    ;;
  (autorecovery)
    echo "doing $startStop $command ..."
    ;;
  (standalone)
    echo "doing $startStop $command ..."
    ;;
  (*)
    echo "Error: unknown service name $command"
    usage
    exit 1
    ;;
esac

export BOOKIE_LOG_DIR=$BOOKIE_LOG_DIR
export BOOKIE_ROOT_LOGGER=$BOOKIE_ROOT_LOGGER
export BOOKIE_LOG_FILE=bookkeeper-$command-$HOSTNAME.log

pid_file="${BOOKIE_PID_DIR}/bookkeeper-${command}.pid"
out=$BOOKIE_LOG_DIR/bookkeeper-$command-$HOSTNAME.out
logfile=$BOOKIE_LOG_DIR/$BOOKIE_LOG_FILE

rotate_out_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
    num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
      prev=$(expr $num - 1)
      [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
      num=$prev
    done
    mv "$log" "$log.$num";
  fi
}

mkdir -p "$BOOKIE_LOG_DIR"

case $startStop in
  (start)
    if [ -f $pid_file ]; then
      PREVIOUS_PID=$(cat $pid_file)
      if kill -0 $PREVIOUS_PID > /dev/null 2>&1; then
        echo $command running as process $PREVIOUS_PID.  Stop it first.
        exit 1
      fi
    fi

    rotate_out_log $out
    echo starting $command, logging to $logfile
    bookkeeper=$BK_HOME/bin/bookkeeper
    nohup $bookkeeper $command "$@" > "$out" 2>&1 < /dev/null &
    echo $! > $pid_file
    sleep 1; head $out
    sleep 2;
    if ! kill -0 $! > /dev/null ; then
      exit 1
    fi
    ;;

  (stop)
    if [ -f $pid_file ]; then
      TARGET_PID=$(cat $pid_file)
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID

        count=0
        location=$BOOKIE_LOG_DIR
        while kill -0 $TARGET_PID > /dev/null 2>&1;
        do
          echo "Shutdown is in progress... Please wait..."
          sleep 1
          count=$(expr $count + 1)

          if [ "$count" = "$BOOKIE_STOP_TIMEOUT" ]; then
            break
          fi
         done

        if [ "$count" != "$BOOKIE_STOP_TIMEOUT" ]; then
          echo "Shutdown completed."
        fi

        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          fileName=$location/$command.out
          $JAVA_HOME/bin/jstack $TARGET_PID > $fileName
          echo Thread dumps are taken for analysis at $fileName
          if [ "$1" == "-force" ]
          then
            echo forcefully stopping $command
            kill -9 $TARGET_PID >/dev/null 2>&1
            echo Successfully stopped the process
          else
            echo "WARNNING :  Bookie Server is not stopped completely."
            exit 1
          fi
        fi
      else
        echo no $command to stop
      fi
      rm $pid_file
    else
      echo no $command to stop
    fi
    ;;

  (*)
    usage
    echo $supportedargs
    exit 1
    ;;
esac
