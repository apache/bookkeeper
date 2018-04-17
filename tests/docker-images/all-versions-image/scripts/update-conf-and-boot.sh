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

mkdir -p $BK_JOURNALDIR $BK_LEDGERDIR

sed -i "s|journalDirectory=.*|journalDirectory=$BK_JOURNALDIR|" /opt/bookkeeper/*/conf/bk_server.conf
sed -i "s|ledgerDirectories=.*|ledgerDirectories=$BK_LEDGERDIR|" /opt/bookkeeper/*/conf/bk_server.conf
sed -i "s|zkServers=.*|zkServers=$BK_ZKCONNECTSTRING|" /opt/bookkeeper/*/conf/bk_server.conf

# 4.3.1 & 4.3.2 shipped with a broken confs
sed -i "s|\(# \)\?logSizeLimit=.*|logSizeLimit=1073741824|" /opt/bookkeeper/4.3.1/conf/bk_server.conf
sed -i "s|\(# \)\?logSizeLimit=.*|logSizeLimit=1073741824|" /opt/bookkeeper/4.3.2/conf/bk_server.conf

# 4.5.1 shipped with a broken conf
sed -i "s|\(# \)\?statsProviderClass=.*|# disabled stats |" /opt/bookkeeper/4.5.1/conf/bk_server.conf

# 4.6.0 breaks supervisor
echo "stopasgroup=true" >> /etc/supervisord/conf.d/bookkeeper-4.6.0.conf

exec /usr/bin/supervisord -c /etc/supervisord.conf
