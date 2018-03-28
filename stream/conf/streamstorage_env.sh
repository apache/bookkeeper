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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting stream storage

# Log4j configuration file
# SS_LOG_CONF=

# Logs location
# SS_LOG_DIR=

# Configuration file of settings used in broker server
# SS_BROKER_CONF=

# Configuration file of settings used in bookie server
# SS_BOOKKEEPER_CONF=

# Configuration file of settings used in zookeeper server
# SS_ZK_CONF=

# Configuration file of settings used in global zookeeper server
# SS_GLOBAL_ZK_CONF=

# Extra options to be passed to the jvm
SS_MEM=" -Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"

# Garbage collection options
SS_GC=" -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB"

# Extra options to be passed to the jvm
SS_EXTRA_OPTS="${SS_EXTRA_OPTS} ${SS_MEM} ${SS_GC} -Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"

# Add extra paths to the bookkeeper classpath
# SS_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#SS_PID_DIR=

#Wait time before forcefully kill the pulser server instance, if the stop is not successful
#SS_STOP_TIMEOUT=
