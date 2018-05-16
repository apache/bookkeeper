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

# Check net.ipv6.bindv6only
if [ -f /sbin/sysctl ]; then
  # check if net.ipv6.bindv6only is set to 1
  bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)
  if [ -n "$bindv6only" ] && [ "$bindv6only" -eq "1" ]
  then
    echo "Error: \"net.ipv6.bindv6only\" is set to 1 - Java networking could be broken"
    echo "For more info (the following page also applies to bookkeeper): http://wiki.apache.org/hadoop/HadoopIPv6"
    exit 1
  fi
fi

# See the following page for extensive details on setting
# up the JVM to accept JMX remote management:
# http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
# by default we allow local JMX connections
if [ "x$JMXLOCALONLY" = "x" ]
then
  JMXLOCALONLY=false
fi

if [ "x$JMXDISABLE" = "x" ]
then
  # for some reason these two options are necessary on jdk6 on Ubuntu
  #   accord to the docs they are not necessary, but otw jconsole cannot
  #   do a local attach
  JMX_ARGS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=$JMXLOCALONLY"
else
  echo "JMX disabled by user request" >&2
fi

# Check for the java to use
if [[ -z ${JAVA_HOME} ]]; then
  JAVA=$(which java)
  if [ $? = 0 ]; then
    echo "JAVA_HOME not set, using java from PATH. ($JAVA)"
  else
    echo "Error: JAVA_HOME not set, and no java executable found in $PATH." 1>&2
    exit 1
  fi
else
  JAVA=${JAVA_HOME}/bin/java
fi

BINDIR=${BK_BINDIR:-"`dirname "$0"`"}
BK_HOME=${BK_HOME:-"`cd ${BINDIR}/..;pwd`"}
BK_CONFDIR=${BK_HOME}/conf
DEFAULT_LOG_CONF=${BK_CONFDIR}/log4j.properties

source ${BK_CONFDIR}/nettyenv.sh
source ${BK_CONFDIR}/bkenv.sh
source ${BK_CONFDIR}/bk_cli_env.sh

# default netty settings
NETTY_LEAK_DETECTION_LEVEL=${NETTY_LEAK_DETECTION_LEVEL:-"disabled"}
NETTY_RECYCLER_MAXCAPACITY=${NETTY_RECYCLER_MAXCAPACITY:-"1000"}
NETTY_RECYCLER_LINKCAPACITY=${NETTY_RECYCLER_LINKCAPACITY:-"1024"}

# default bookie JVM settings
DEFAULT_BOOKIE_GC_OPTS="-XX:+UseG1GC \
    -XX:MaxGCPauseMillis=10 \
    -XX:+ParallelRefProcEnabled \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+AggressiveOpts \
    -XX:+DoEscapeAnalysis \
    -XX:ParallelGCThreads=32 \
    -XX:ConcGCThreads=32 \
    -XX:G1NewSizePercent=50 \
    -XX:+DisableExplicitGC \
    -XX:-ResizePLAB"
DEFAULT_BOOKIE_GC_LOGGING_OPTS="-XX:+PrintGCDetails \
    -XX:+PrintGCApplicationStoppedTime  \
    -XX:+UseGCLogFileRotation \
    -XX:NumberOfGCLogFiles=5 \
    -XX:GCLogFileSize=64m"
BOOKIE_MAX_HEAP_MEMORY=${BOOKIE_MAX_HEAP_MEMORY:-"1g"}
BOOKIE_MIN_HEAP_MEMORY=${BOOKIE_MIN_HEAP_MEMORY:-"1g"}
BOOKIE_MAX_DIRECT_MEMORY=${BOOKIE_MAX_DIRECT_MEMORY:-"2g"}
BOOKIE_MEM_OPTS=${BOOKIE_MEM_OPTS:-"-Xms${BOOKIE_MIN_HEAP_MEMORY} -Xmx${BOOKIE_MAX_HEAP_MEMORY} -XX:MaxDirectMemorySize=${BOOKIE_MAX_DIRECT_MEMORY}"}
BOOKIE_GC_OPTS=${BOOKIE_GC_OPTS:-"${DEFAULT_BOOKIE_GC_OPTS}"}
BOOKIE_GC_LOGGING_OPTS=${BOOKIE_GC_LOGGING_OPTS:-"${DEFAULT_BOOKIE_GC_LOGGING_OPTS}"}

# default CLI JVM settings
DEFAULT_CLI_GC_OPTS="-XX:+UseG1GC \
    -XX:MaxGCPauseMillis=10"
DEFAULT_CLI_GC_LOGGING_OPTS="-XX:+PrintGCDetails \
    -XX:+PrintGCApplicationStoppedTime  \
    -XX:+UseGCLogFileRotation \
    -XX:NumberOfGCLogFiles=5 \
    -XX:GCLogFileSize=64m"
CLI_MAX_HEAP_MEMORY=${CLI_MAX_HEAP_MEMORY:-"512M"}
CLI_MIN_HEAP_MEMORY=${CLI_MIN_HEAP_MEMORY:-"256M"}
CLI_MEM_OPTS=${CLI_MEM_OPTS:-"-Xms${CLI_MIN_HEAP_MEMORY} -Xmx${CLI_MAX_HEAP_MEMORY}"}
CLI_GC_OPTS=${CLI_GC_OPTS:-"${DEFAULT_CLI_GC_OPTS}"}
CLI_GC_LOGGING_OPTS=${CLI_GC_LOGGING_OPTS:-"${DEFAULT_CLI_GC_LOGGING_OPTS}"}

find_module_jar_at() {
  DIR=$1
  MODULE=$2
  REGEX="^${MODULE}-[0-9\\.]*(-SNAPSHOT)?.jar$"
  if [ -d ${DIR} ]; then
    cd ${DIR}
    for f in *.jar; do
      if [[ ${f} =~ ${REGEX} ]]; then
        echo ${DIR}/${f}
        return
      fi
    done
  fi
}

find_module_jar() {
  MODULE_PATH=$1
  MODULE_NAME=$2
  RELEASE_JAR=$(find_module_jar_at ${BK_HOME} ${MODULE_NAME})
  if [ -n "${RELEASE_JAR}" ]; then
    MODULE_JAR=${RELEASE_JAR}
  else
    RELEASE_JAR=$(find_module_jar_at ${BK_HOME}/lib ${MODULE_NAME})
    if [ -n "${RELEASE_JAR}" ]; then
      MODULE_JAR=${RELEASE_JAR}
    fi
  fi

  if [ -z "${MODULE_JAR}" ]; then
    BUILT_JAR=$(find_module_jar_at ${BK_HOME}/${MODULE_PATH}/target ${MODULE_NAME})
    if [ -z "${BUILT_JAR}" ]; then
      echo "Couldn't find module '${MODULE_NAME}' jar." >&2
      read -p "Do you want me to run \`mvn package -DskiptTests\` for you ? " answer
      case "${answer:0:1}" in
        y|Y )
          mvn package -DskipTests
          ;;
        * )
          exit 1
          ;;
      esac

      BUILT_JAR=$(find_module_jar_at ${BK_HOME}/${MODULE_PATH}/target ${MODULE_NAME})
    fi
    if [ -n "${BUILT_JAR}" ]; then
      MODULE_JAR=${BUILT_JAR}
    fi
  fi

  if [ ! -e "${MODULE_JAR}" ]; then
    echo "Could not find module '${MODULE_JAR}' jar." >&2
    exit 1
  fi
  echo ${MODULE_JAR}
  return
}

add_maven_deps_to_classpath() {
  MODULE_PATH=$1
  MVN="mvn"
  if [ "$MAVEN_HOME" != "" ]; then
    MVN=${MAVEN_HOME}/bin/mvn
  fi

  # Need to generate classpath from maven pom. This is costly so generate it
  # and cache it. Save the file into our target dir so a mvn clean will get
  # clean it up and force us create a new one.
  f="${MODULE_PATH}/target/cached_classpath.txt"
  if [ ! -f ${f} ]; then
    ${MVN} -f "${MODULE_PATH}/pom.xml" dependency:build-classpath -Dmdep.outputFile="target/cached_classpath.txt" &> /dev/null
  fi
}

set_module_classpath() {
  MODULE_PATH=$1
  if [ -d "${BK_HOME}/lib" ]; then
    BK_CLASSPATH=""
    for i in ${BK_HOME}/lib/*.jar; do
      BK_CLASSPATH=${BK_CLASSPATH}:${i}
    done
    echo ${BK_CLASSPATH}
  else
    add_maven_deps_to_classpath ${MODULE_PATH} >&2
    cat ${MODULE_PATH}/target/cached_classpath.txt
  fi
  return
}

build_bookie_jvm_opts() {
  LOG_DIR=$1
  GC_LOG_FILENAME=$2

  echo "$BOOKIE_MEM_OPTS $BOOKIE_GC_OPTS $BOOKIE_GC_LOGGING_OPTS $BOOKIE_PERF_OPTS -Xloggc:${LOG_DIR}/${GC_LOG_FILENAME}"
}

build_cli_jvm_opts() {
  LOG_DIR=$1
  GC_LOG_FILENAME=$2

  echo "$CLI_MEM_OPTS $CLI_GC_OPTS $CLI_GC_LOGGING_OPTS -Xloggc:${LOG_DIR}/${GC_LOG_FILENAME}"
}

build_netty_opts() {
  echo "-Dio.netty.leakDetectionLevel=${NETTY_LEAK_DETECTION_LEVEL} \
    -Dio.netty.recycler.maxCapacity.default=${NETTY_RECYCLER_MAXCAPACITY} \
    -Dio.netty.recycler.linkCapacity=${NETTY_RECYCLER_LINKCAPACITY}"
}

build_logging_opts() {
  CONF_FILE=$1
  LOG_DIR=$2
  LOG_FILE=$3
  LOGGER=$4

  echo "-Dlog4j.configuration=`basename ${CONF_FILE}` \
    -Dbookkeeper.root.logger=${LOGGER} \
    -Dbookkeeper.log.dir=${LOG_DIR} \
    -Dbookkeeper.log.file=${LOG_FILE}"
}

build_cli_logging_opts() {
  CONF_FILE=$1
  LOG_DIR=$2
  LOG_FILE=$3
  LOGGER=$4

  echo "-Dlog4j.configuration=`basename ${CONF_FILE}` \
    -Dbookkeeper.cli.root.logger=${LOGGER} \
    -Dbookkeeper.cli.log.dir=${LOG_DIR} \
    -Dbookkeeper.cli.log.file=${LOG_FILE}"
}

build_bookie_opts() {
  echo "-Djava.net.preferIPv4Stack=true"
}
