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
Usage: hedwig-daemon.sh (start|stop) <command> <args...>
where command is one of:
    server           Run the hedwig server
EOF
}


BINDIR=`dirname "$0"`
HEDWIG_HOME=`cd $BINDIR/..;pwd`

if [ -f $HEDWIG_HOME/conf/hwenv.sh ]
then
 . $HEDWIG_HOME/conf/hwenv.sh
fi

HEDWIG_LOG_DIR=${HEDWIG_LOG_DIR:-"$HEDWIG_HOME/logs"}

HEDWIG_ROOT_LOGGER=${HEDWIG_ROOT_LOGGER:-'INFO,ROLLINGFILE'}

HEDWIG_STOP_TIMEOUT=${HEDWIG_STOP_TIMEOUT:-30}

HEDWIG_PID_DIR=${HEDWIG_PID_DIR:-$HEDWIG_HOME/bin}

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
    (server)
        echo "doing $startStop $command ..."
        ;;
    (*)
        echo "Error: unknown service name $command"
        usage
        exit 1
        ;;
esac

export HEDWIG_LOG_DIR=$HEDWIG_LOG_DIR
export HEDWIG_ROOT_LOGGER=$HEDWIG_ROOT_LOGGER
export HEDWIG_LOG_FILE=hedwig-$command-$HOSTNAME.log

pid=$HEDWIG_PID_DIR/hedwig-$command.pid
out=$HEDWIG_LOG_DIR/hedwig-$command-$HOSTNAME.out
logfile=$HEDWIG_LOG_DIR/$HEDWIG_LOG_FILE

rotate_out_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
       num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

mkdir -p "$HEDWIG_LOG_DIR"

case $startStop in
  (start)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    rotate_out_log $out
    echo starting $command, logging to $logfile
    hedwig=$HEDWIG_HOME/bin/hedwig
    nohup $hedwig $command "$@" > "$out" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head $out
    sleep 2;
    if ! ps -p $! > /dev/null ; then
      exit 1
    fi
    ;;

  (stop)
    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID

        count=0
        location=$HEDWIG_LOG_DIR
        while ps -p $TARGET_PID > /dev/null;
         do
          echo "Shutdown is in progress... Please wait..."
          sleep 1
          count=`expr $count + 1`
         
          if [ "$count" = "$HEDWIG_STOP_TIMEOUT" ]; then
                break
          fi
         done
        
        if [ "$count" != "$HEDWIG_STOP_TIMEOUT" ]; then
                 echo "Shutdown completed."
                exit 0
        fi
                 
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
              fileName=$location/$command.out
              $JAVA_HOME/bin/jstack $TARGET_PID > $fileName
              echo Thread dumps are taken for analysis at $fileName
              echo forcefully stopping $command
              kill -9 $TARGET_PID >/dev/null 2>&1
              echo Successfully stopped the process
        fi
      else
        echo no $command to stop
      fi
      rm $pid
    else
      echo no $command to stop
    fi
    ;;

  (*)
    usage
    exit 1
    ;;
esac
