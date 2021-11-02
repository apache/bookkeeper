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

ARGV0=`basename "$0"`
PREFIX="bk_test_"
SHELLS="/bin/sh /bin/bash"

find_tests_at() {
  DIR=$1
  PREF=$2
  REGEX="^${PREF}[a-z_]*.sh$"
  RESULTS=""
  if [ -d ${DIR} ]; then
    cd ${DIR}
    for f in *.sh; do
      if [[ ${f} =~ ${REGEX} ]]; then
        RESULTS="${RESULTS} ${f}"
      fi
    done
  fi
  echo ${RESULTS}
}

TESTS=$(find_tests_at "." ${PREFIX})

# load common unit test functions
source ./versions
source ./bk_test_helpers

usage() {
  echo "usage: ${ARGV0} [-e key=val ...] [-s shell(s)] [-t test(s)]"
}

env=''

# process command line flags
while getopts 'e:hs:t:' opt; do
  case ${opt} in
    e)  # set an environment variable
      key=`expr "${OPTARG}" : '\([^=]*\)='`
      val=`expr "${OPTARG}" : '[^=]*=\(.*\)'`
      if [ -z "${key}" -o -z "${val}" ]; then
        usage
        exit 1
      fi
      eval "${key}='${val}'"
      export ${key}
      env="${env:+${env} }${key}"
      ;;
    h) usage; exit 0 ;;  # output help
    s) shells=${OPTARG} ;;  # list of shells to run
    t) tests=${OPTARG} ;;  # list of tests to run
    *) usage; exit 1 ;;
  esac
done
shift `expr ${OPTIND} - 1`

# fill shells and/or tests
shells=${shells:-${SHELLS}}
tests=${tests:-${TESTS}}

# error checking
if [ -z "${tests}" ]; then
  bk_info 'no tests found to run; exiting'
  exit 0
fi

# print run info
cat <<EOF
#------------------------------------------------------------------------------
# System data
#
# test run info
shells="${shells}"
tests="${tests}"
EOF
for key in ${env}; do
  eval "echo \"${key}=\$${key}\""
done
echo

# output system data
echo "# system info"
echo "$ date"
date

echo "$ uname -mprsv"
uname -mprsv

#
# run tests
#

for shell in ${shells}; do
  echo

  # check for existance of shell
  if [ ! -x ${shell} ]; then
    bk_warn "unable to run tests with the ${shell} shell"
    continue
  fi

  cat <<EOF
#------------------------------------------------------------------------------
# Running the test suite with ${shell}
#
EOF

  shell_name=`basename ${shell}`
  shell_version=`versions_shellVersion "${shell}"`

  echo "shell name: ${shell_name}"
  echo "shell version: ${shell_version}"

  # execute the tests
  for suite in ${tests}; do
    suiteName=`expr "${suite}" : "${PREFIX}\(.*\).sh"`
    echo
    echo "--- Executing the '${suiteName}' test suite ---"
    ( exec ${shell} ./${suite} 2>&1; )
  done
done
