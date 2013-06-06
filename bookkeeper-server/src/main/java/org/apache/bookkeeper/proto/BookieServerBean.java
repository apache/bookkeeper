/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.proto;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.proto.BKStats;
import org.apache.bookkeeper.proto.BKStats.OpStats;
import org.apache.bookkeeper.proto.BKStats.OpStatData;

/**
 * Bookie Server Bean
 */
public class BookieServerBean implements BookieServerMXBean, BKMBeanInfo {

    protected final BookieServer bks;
    protected final ServerConfiguration conf;
    private final String name;

    public BookieServerBean(ServerConfiguration conf, BookieServer bks) {
        this.conf = conf;
        this.bks = bks;
        name = "BookieServer_" + conf.getBookiePort();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public long getNumPacketsReceived() {
        return ServerStats.getInstance().getPacketsReceived();
    }

    @Override
    public long getNumPacketsSent() {
        return ServerStats.getInstance().getPacketsSent();
    }

    @Override
    public OpStatData getAddStats() {
        return bks.bkStats.getOpStats(BKStats.STATS_ADD).toOpStatData();
    }

    @Override
    public OpStatData getReadStats() {
        return bks.bkStats.getOpStats(BKStats.STATS_READ).toOpStatData();
    }

    @Override
    public String getServerPort() {
        try {
            return StringUtils.addrToString(Bookie.getBookieAddress(conf));
        } catch (UnknownHostException e) {
            return "localhost:" + conf.getBookiePort();
        }
    }

}
