#!/bin/sh
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

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

########################################
# default settings for bookkeeper
########################################

# Configuration file of settings used in bookie server
# BOOKIE_CONF=

# Configuration file of settings used in zookeeper server
# BOOKIE_ZK_CONF=

# Extra options to be passed to the jvm
# BOOKIE_EXTRA_OPTS=

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

# Folder where the Bookie server PID file should be stored
# BOOKIE_PID_DIR=

# Wait time before forcefully kill the Bookie server instance, if the stop is not successful
# BOOKIE_STOP_TIMEOUT=

# this default config dir should match the 'localBookiesConfigDirectory' config value in the conf file of LocalBookKeeper
# LOCALBOOKIES_CONFIG_DIR=/tmp/localbookies-config

#################################
# BookKeeper Logging Options
#################################

# Log4j configuration file
# BOOKIE_LOG_CONF=

# Logs location
# BOOKIE_LOG_DIR=

# Log file name
# BOOKIE_LOG_FILE="bookkeeper.log"

# Log level & appender
# BOOKIE_ROOT_LOG_LEVEL="INFO"
# BOOKIE_ROOT_LOG_APPENDER="CONSOLE"

#################################
# BookKeeper JVM memory options
#################################

# BOOKIE_MAX_HEAP_MEMORY=1g
# BOOKIE_MIN_HEAP_MEMORY=1g
# BOOKIE_MAX_DIRECT_MEMORY=2g
# BOOKIE_MEM_OPTS=

# JVM GC options
# BOOKIE_GC_OPTS=

# JVM GC logging options
# BOOKIE_GC_LOGGING_OPTS=

# JVM performance options
# BOOKIE_PERF_OPTS="-XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch -XX:-UseBiasedLocking"
