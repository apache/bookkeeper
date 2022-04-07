#!/usr/bin/env bash
#
# vim:et:ft=sh:sts=2:sw=2
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
# load test helpers
. ./bk_test_helpers

#------------------------------------------------------------------------------
# suite tests
#

testDefaultVariables() {
  source ${BK_BINDIR}/common_gradle.sh
  assertEquals "BINDIR is not set correctly" "${BK_BINDIR}" "${BINDIR}"
  assertEquals "BK_HOME is not set correctly" "${BK_HOMEDIR}" "${BK_HOME}"
  assertEquals "DEFAULT_LOG_CONF is not set correctly" "${BK_CONFDIR}/log4j.properties" "${DEFAULT_LOG_CONF}"
  assertEquals "NETTY_LEAK_DETECTION_LEVEL is not set correctly" "disabled" "${NETTY_LEAK_DETECTION_LEVEL}"
  assertEquals "NETTY_RECYCLER_MAXCAPACITY is not set correctly" "1000" "${NETTY_RECYCLER_MAXCAPACITY}"
  assertEquals "NETTY_RECYCLER_LINKCAPACITY is not set correctly" "1024" "${NETTY_RECYCLER_LINKCAPACITY}"
  assertEquals "BOOKIE_MAX_HEAP_MEMORY is not set correctly" "1g" "${BOOKIE_MAX_HEAP_MEMORY}"
  assertEquals "BOOKIE_MIN_HEAP_MEMORY is not set correctly" "1g" "${BOOKIE_MIN_HEAP_MEMORY}"
  assertEquals "BOOKIE_MAX_DIRECT_MEMORY is not set correctly" "2g" "${BOOKIE_MAX_DIRECT_MEMORY}"
  assertEquals "BOOKIE_MEM_OPTS is not set correctly" "-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g" "${BOOKIE_MEM_OPTS}"
  assertEquals "BOOKIE_GC_OPTS is not set correctly" "${DEFAULT_BOOKIE_GC_OPTS}" "${BOOKIE_GC_OPTS}"
  assertEquals "BOOKIE_GC_LOGGING_OPTS is not set correctly" "${DEFAULT_BOOKIE_GC_LOGGING_OPTS}" "${BOOKIE_GC_LOGGING_OPTS}"
  assertEquals "CLI_MAX_HEAP_MEMORY is not set correctly" "512M" "${CLI_MAX_HEAP_MEMORY}"
  assertEquals "CLI_MIN_HEAP_MEMORY is not set correctly" "256M" "${CLI_MIN_HEAP_MEMORY}"
  assertEquals "CLI_MEM_OPTS is not set correctly" "-Xms256M -Xmx512M" "${CLI_MEM_OPTS}"
  assertEquals "CLI_GC_OPTS is not set correctly" "${DEFAULT_CLI_GC_OPTS}" "${CLI_GC_OPTS}"
  assertEquals "CLI_GC_LOGGING_OPTS is not set correctly" "${DEFAULT_CLI_GC_LOGGING_OPTS}" "${CLI_GC_LOGGING_OPTS}"
}

testFindModuleJarAt() {
  source ${BK_BINDIR}/common_gradle.sh

  MODULE="test-module"

  # case 1: empty dir
  TEST_DIR1=${BK_TMPDIR}/testdir1
  mkdir -p ${TEST_DIR1}
  MODULE_JAR1=$(find_module_jar_at ${TEST_DIR1} ${MODULE})
  assertEquals "No module jar should be found at empty dir" "" "${MODULE_JAR1}"

  # case 2: SNAPSHOT jar
  TEST_FILES=(
    "invalid-${MODULE}.jar"
    "invalid-${MODULE}-4.8.0.jar"
    "invalid-${MODULE}-4.8.0-SNAPSHOT.jar"
    "${MODULE}.jar.invalid"
    "${MODULE}-4.8.0.jar.invalid"
    "${MODULE}-4.8.0-SNAPSHOT.jar.invalid"
    "${MODULE}.jar"
    "${MODULE}-4.8.0-SNAPSHOT.jar"
  )

  TEST_DIR2=${BK_TMPDIR}/testdir2
  mkdir -p ${TEST_DIR2}
  count=0
  while [ "x${TEST_FILES[count]}" != "x" ]
  do
    touch ${TEST_DIR2}/${TEST_FILES[count]}
    count=$(( $count + 1 ))
  done
  MODULE_JAR2=$(find_module_jar_at ${TEST_DIR2} ${MODULE})
  assertEquals "${MODULE}-4.8.0-SNAPSHOT.jar is not found" "${TEST_DIR2}/${MODULE}-4.8.0-SNAPSHOT.jar" "${MODULE_JAR2}"

  # case 3: release jar
  TEST_FILES=(
    "invalid-${MODULE}.jar"
    "invalid-${MODULE}-4.8.0.jar"
    "invalid-${MODULE}-4.8.0-SNAPSHOT.jar"
    "${MODULE}.jar.invalid"
    "${MODULE}-4.8.0.jar.invalid"
    "${MODULE}-4.8.0-SNAPSHOT.jar.invalid"
    "${MODULE}.jar"
    "${MODULE}-4.8.0.jar"
  )

  TEST_DIR3=${BK_TMPDIR}/testdir3
  mkdir -p ${TEST_DIR3}
  count=0
  while [ "x${TEST_FILES[count]}" != "x" ]
  do
    touch ${TEST_DIR3}/${TEST_FILES[count]}
    count=$(( $count + 1 ))
  done
  MODULE_JAR3=$(find_module_jar_at ${TEST_DIR3} ${MODULE})
  assertEquals "${MODULE}-4.8.0.jar is not found" "${TEST_DIR3}/${MODULE}-4.8.0.jar" "${MODULE_JAR3}"
}

testFindModuleJar() {
  BK_HOME=${BK_TMPDIR}
  # prepare the env files
  mkdir -p ${BK_HOME}/conf
  echo "" > ${BK_HOME}/conf/nettyenv.sh
  echo "" > ${BK_HOME}/conf/bkenv.sh
  echo "" > ${BK_HOME}/conf/bk_cli_env.sh

  source ${BK_BINDIR}/common_gradle.sh

  MODULE="test-module"
  MODULE_PATH="testmodule"
  VERSION="4.8.0"

  TEST_FILES=(
    "${MODULE}-${VERSION}.jar"
    "lib/${MODULE}-${VERSION}.jar"
    "${MODULE_PATH}/build/libs/${MODULE}-${VERSION}.jar"
  )
  count=0
  while [ "x${TEST_FILES[count]}" != "x" ]
  do
    DIR=`dirname ${BK_TMPDIR}/${TEST_FILES[count]}`
    mkdir -p ${DIR}
    touch ${BK_TMPDIR}/${TEST_FILES[count]}
    count=$(( $count + 1 ))
  done

  count=0
  while [ "x${TEST_FILES[count]}" != "x" ]
  do
    FILE="${BK_TMPDIR}/${TEST_FILES[count]}"
    ACTUAL_FILE=$(find_module_jar ${MODULE_PATH} ${MODULE})

    assertEquals "Module file is not found" "${FILE}" "${ACTUAL_FILE}"

    # delete the file
    rm ${FILE}
    count=$(( $count + 1 ))
  done

  unset BK_HOME
}

testLoadEnvfiles() {
  BK_HOME=${BK_TMPDIR}

  # prepare the env files
  mkdir -p ${BK_HOME}/conf
  echo "NETTY_LEAK_DETECTION_LEVEL=enabled" > ${BK_HOME}/conf/nettyenv.sh
  echo "BOOKIE_MAX_HEAP_MEMORY=2048M" > ${BK_HOME}/conf/bkenv.sh
  echo "CLI_MAX_HEAP_MEMORY=2048M" > ${BK_HOME}/conf/bk_cli_env.sh

  # load the common_gradle.sh
  source ${BK_BINDIR}/common_gradle.sh

  assertEquals "NETTY_LEAK_DETECTION_LEVEL is not set correctly" "enabled" "${NETTY_LEAK_DETECTION_LEVEL}"
  assertEquals "BOOKIE_MAX_HEAP_MEMORY is not set correctly" "2048M" "${BOOKIE_MAX_HEAP_MEMORY}"
  assertEquals "CLI_MAX_HEAP_MEMORY is not set correctly" "2048M" "${CLI_MAX_HEAP_MEMORY}"

  unset NETTY_LEAK_DETECTION_LEVEL
  unset BOOKIE_MAX_HEAP_MEMORY
  unset CLI_MAX_HEAP_MEMORY
  unset BK_HOME
}

testBuildBookieJVMOpts() {
  source ${BK_BINDIR}/common_gradle.sh

  TEST_LOG_DIR=${BK_TMPDIR}/logdir
  TEST_GC_LOG_FILENAME="test-gc.log"
  ACTUAL_JVM_OPTS=$(build_bookie_jvm_opts ${TEST_LOG_DIR} ${TEST_GC_LOG_FILENAME})
  USEJDK8=$(detect_jdk8)
  if [ "$USING_JDK8" -ne "1" ]; then
    EXPECTED_JVM_OPTS="-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g ${DEFAULT_BOOKIE_GC_OPTS} ${DEFAULT_BOOKIE_GC_LOGGING_OPTS}  -Xlog:gc=info:file=${TEST_LOG_DIR}/${TEST_GC_LOG_FILENAME}::filecount=5,filesize=64m"
  else
    EXPECTED_JVM_OPTS="-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g ${DEFAULT_BOOKIE_GC_OPTS} ${DEFAULT_BOOKIE_GC_LOGGING_OPTS}  -Xloggc:${TEST_LOG_DIR}/${TEST_GC_LOG_FILENAME}"
  fi
  assertEquals "JVM OPTS is not set correctly" "${EXPECTED_JVM_OPTS}" "${ACTUAL_JVM_OPTS}"
}

testBuildCLIJVMOpts() {
  source ${BK_BINDIR}/common_gradle.sh

  TEST_LOG_DIR=${BK_TMPDIR}/logdir
  TEST_GC_LOG_FILENAME="test-gc.log"
  ACTUAL_JVM_OPTS=$(build_cli_jvm_opts ${TEST_LOG_DIR} ${TEST_GC_LOG_FILENAME})
  USEJDK8=$(detect_jdk8)
  if [ "$USING_JDK8" -ne "1" ]; then
    EXPECTED_JVM_OPTS="-Xms256M -Xmx512M ${DEFAULT_CLI_GC_OPTS} ${DEFAULT_CLI_GC_LOGGING_OPTS} -Xlog:gc=info:file=${TEST_LOG_DIR}/${TEST_GC_LOG_FILENAME}::filecount=5,filesize=64m"
  else
    EXPECTED_JVM_OPTS="-Xms256M -Xmx512M ${DEFAULT_CLI_GC_OPTS} ${DEFAULT_CLI_GC_LOGGING_OPTS} -Xloggc:${TEST_LOG_DIR}/${TEST_GC_LOG_FILENAME}"
  fi
  assertEquals "JVM OPTS is not set correctly" "${EXPECTED_JVM_OPTS}" "${ACTUAL_JVM_OPTS}"
}

testBuildNettyOpts() {
  source ${BK_BINDIR}/common_gradle.sh

  ACTUAL_NETTY_OPTS=$(build_netty_opts)
  EXPECTED_NETTY_OPTS="-Dio.netty.leakDetectionLevel=disabled"

    assertEquals "Netty OPTS is not set correctly" "${EXPECTED_NETTY_OPTS}" "${ACTUAL_NETTY_OPTS}"
}

testBuildBookieOpts() {
  source ${BK_BINDIR}/common_gradle.sh

  ACTUAL_OPTS=$(build_bookie_opts)
  EXPECTED_OPTS="-Djava.net.preferIPv4Stack=true"

  assertEquals "Bookie OPTS is not set correctly" "${EXPECTED_OPTS}" "${ACTUAL_OPTS}"
}

testBuildLoggingOpts() {
  TEST_CONF_FILE="test.conf"
  TEST_LOG_DIR="test_log_dir"
  TEST_LOG_FILE="test_log_file"
  TEST_LOGGER="INFO,TEST"

  EXPECTED_OPTS="-Dlog4j.configuration=${TEST_CONF_FILE} \
    -Dbookkeeper.root.logger=${TEST_LOGGER} \
    -Dbookkeeper.log.dir=${TEST_LOG_DIR} \
    -Dbookkeeper.log.file=${TEST_LOG_FILE}"
  ACTUAL_OPTS=$(build_logging_opts ${TEST_CONF_FILE} ${TEST_LOG_DIR} ${TEST_LOG_FILE} ${TEST_LOGGER})

  assertEquals "Logging OPTS is not set correctly" "${EXPECTED_OPTS}" "${ACTUAL_OPTS}"
}

testBuildCLILoggingOpts() {
  TEST_CONF_FILE="test.conf"
  TEST_LOG_DIR="test_log_dir"
  TEST_LOG_FILE="test_log_file"
  TEST_LOGGER="INFO,TEST"

  EXPECTED_OPTS="-Dlog4j.configuration=${TEST_CONF_FILE} \
    -Dbookkeeper.cli.root.logger=${TEST_LOGGER} \
    -Dbookkeeper.cli.log.dir=${TEST_LOG_DIR} \
    -Dbookkeeper.cli.log.file=${TEST_LOG_FILE}"
  ACTUAL_OPTS=$(build_cli_logging_opts ${TEST_CONF_FILE} ${TEST_LOG_DIR} ${TEST_LOG_FILE} ${TEST_LOGGER})

  assertEquals "Logging OPTS is not set correctly" "${EXPECTED_OPTS}" "${ACTUAL_OPTS}"
}

#------------------------------------------------------------------------------
# suite functions
#

oneTimeSetUp() {
  bk_oneTimeSetUp
}

# load and run shUnit2
. ${BK_SHUNIT}
