#!/bin/sh
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

java -Xms1G -Xmx1G -Djdk.nio.maxCachedBufferSize=0 -Djava.net.preferIPv4Stack=true -Duser.timezone=UTC -XX:-MaxFDLimit -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ResizeTLAB -XX:-ResizePLAB -XX:MetaspaceSize=128m -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ParallelRefProcEnabled -XX:StackShadowPages=20 -XX:+UseCompressedOops -XX:+DisableExplicitGC -XX:StringTableSize=1000003 -XX:InitiatingHeapOccupancyPercent=40 -jar target/benchmarks.jar $@ -prof gc -prof stack:lines=5;time=1;top=3

