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

package org.apache.hedwig.server.netty;

import org.apache.hedwig.server.jmx.HedwigMBeanInfo;
import org.apache.hedwig.server.netty.ServerStats.OpStatData;

import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

/**
 * PubSub Server Bean
 */
public class PubSubServerBean implements PubSubServerMXBean, HedwigMBeanInfo {

    private final String name;

    public PubSubServerBean(String jmxName) {
        this.name = jmxName;
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
    public OpStatData getPubStats() {
        return ServerStats.getInstance().getOpStats(OperationType.PUBLISH).toOpStatData();
    }

    @Override
    public OpStatData getSubStats() {
        return ServerStats.getInstance().getOpStats(OperationType.SUBSCRIBE).toOpStatData();
    }

    @Override
    public OpStatData getUnsubStats() {
        return ServerStats.getInstance().getOpStats(OperationType.UNSUBSCRIBE).toOpStatData();
    }

    @Override
    public OpStatData getConsumeStats() {
        return ServerStats.getInstance().getOpStats(OperationType.CONSUME).toOpStatData();
    }

    @Override
    public long getNumRequestsReceived() {
        return ServerStats.getInstance().getNumRequestsReceived();
    }

    @Override
    public long getNumRequestsRedirect() {
        return ServerStats.getInstance().getNumRequestsRedirect();
    }

    @Override
    public long getNumMessagesDelivered() {
        return ServerStats.getInstance().getNumMessagesDelivered();
    }


}
