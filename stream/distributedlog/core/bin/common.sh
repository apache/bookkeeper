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

if [ $(uname) == "Linux" ]; then
  # check if net.ipv6.bindv6only is set to 1
  bindv6only=$(/sbin/sysctl -n net.ipv6.bindv6only 2> /dev/null)
  if [ -n "${bindv6only}" ] && [ "${bindv6only}" -eq "1" ]; then
    echo "Error: \"net.ipv6.bindv6only\" is set to 1 - Java networking could be broken"
    echo "For more info (the following page also applies to DistributedLog): http://wiki.apache.org/hadoop/HadoopIPv6"
    exit 1
  fi
fi

# See the following page for extensive details on setting
# up the JVM to accept JMX remote management:
# http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
# by default we allow local JMX connections
if [ -z "${JMXLOCALONLY}" ]; then
  JMXLOCALONLY=false
fi

if [ -z "${JMXDISABLE}" ]; then
  echo "JMX enabled by default" >&2
  # for some reason these two options are necessary on jdk6 on Ubuntu
  # accord to the docs they are not necessary, but otw jconsole cannot
  # do a local attach
  JMX_ARGS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=${JMXLOCALONLY}"
else
  echo "JMX disabled by user request" >&2
fi

echo "DLOG_HOME => ${DLOG_HOME}"
DEFAULT_LOG_CONF="${DLOG_HOME}/conf/log4j.properties"

[ -f "${DLOG_HOME}/conf/dlogenv.sh" ] && source "${DLOG_HOME}/conf/dlogenv.sh"

# exclude tests jar
RELEASE_JAR=$(ls ${DLOG_HOME}/distributedlog-*.jar 2> /dev/null | grep -v 'tests\|javadoc\|sources' | tail -1)
if [ $? == 0 ]; then
  DLOG_JAR="${RELEASE_JAR}"
fi

# exclude tests jar
BUILT_JAR=$(ls ${DLOG_HOME}/target/distributedlog-*.jar 2> /dev/null | grep -v 'tests\|javadoc\|sources' | tail -1)

if [ -e "${BUILD_JAR}" ] && [ -e "${DLOG_JAR}" ]; then
  echo "\nCouldn't find dlog jar.";
  echo "Make sure you've run 'mvn package'\n";
  exit 1;
elif [ -e "${BUILT_JAR}" ]; then
  DLOG_JAR="${BUILT_JAR}"
fi

add_maven_deps_to_classpath() {
  MVN="mvn"
  if [ -n "${MAVEN_HOME}" ]; then
    MVN="${MAVEN_HOME}/bin/mvn"
  fi

  # Need to generate classpath from maven pom. This is costly so generate it
  # and cache it. Save the file into our target dir so a mvn clean will get
  # clean it up and force us create a new one.
  f="${PWD}/${DLOG_HOME}/target/cached_classpath.txt"
  if [ ! -f "${f}" ]; then
    "${MVN}" -f "${DLOG_HOME}/pom.xml" dependency:build-classpath -Dmdep.outputFile="${f}" &> /dev/null
  fi
  DLOG_CLASSPATH="${CLASSPATH}":$(cat "${f}")
}

if [ -d "${DLOG_HOME}/lib" ]; then
  for i in ${DLOG_HOME}/lib/*.jar; do
    DLOG_CLASSPATH="${DLOG_CLASSPATH}:${i}"
  done
else
  add_maven_deps_to_classpath
fi

# if no args specified, exit
if [ $# = 0 ]; then
  exit 1
fi

if [ -z "${DLOG_LOG_CONF}" ]; then
  DLOG_LOG_CONF="${DEFAULT_LOG_CONF}"
fi

DLOG_CLASSPATH="${DLOG_JAR}:${DLOG_CLASSPATH}:${DLOG_EXTRA_CLASSPATH}"
if [ -n "${DLOG_LOG_CONF}" ]; then
  DLOG_CLASSPATH="$(dirname ${DLOG_LOG_CONF}):${DLOG_CLASSPATH}"
  OPTS="${OPTS} -Dlog4j.configuration=$(basename ${DLOG_LOG_CONF})"
fi
OPTS="-cp ${DLOG_CLASSPATH} ${OPTS} ${DLOG_EXTRA_OPTS}"

OPTS="${OPTS} ${DLOG_EXTRA_OPTS}"

# Disable ipv6 as it can cause issues
OPTS="${OPTS} -Djava.net.preferIPv4Stack=true"

# log directory & file
DLOG_ROOT_LOGGER=${DLOG_ROOT_LOGGER:-"INFO,R"}
DLOG_LOG_DIR=${DLOG_LOG_DIR:-"$DLOG_HOME/logs"}
DLOG_LOG_FILE=${DLOG_LOG_FILE:-"dlog.log"}

#Configure log configuration system properties
OPTS="$OPTS -Ddlog.root.logger=${DLOG_ROOT_LOGGER}"
OPTS="$OPTS -Ddlog.log.dir=${DLOG_LOG_DIR}"
OPTS="$OPTS -Ddlog.log.file=${DLOG_LOG_FILE}"
