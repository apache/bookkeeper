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

package org.apache.hedwig.server.topics;

import org.apache.hedwig.util.HedwigSocketAddress;

import org.junit.Test;
import org.junit.Assert;

public class TestHubInfo {

    @Test
    public void testParseHubInfo() throws Exception {
        HedwigSocketAddress addr = new HedwigSocketAddress("localhost", 9086, 9087);
        HubInfo hubInfo1 = new HubInfo(addr, 9999);

        String strHubInfo1 = hubInfo1.toString();
        HubInfo parsedHubInfo1 = HubInfo.parse(strHubInfo1);
        Assert.assertEquals("Hub infos should be same", hubInfo1, parsedHubInfo1);

        HubInfo hubInfo2 = new HubInfo(addr, 0);
        HubInfo parsedHubInfo2 = HubInfo.parse("localhost:9086:9087");
        Assert.assertEquals("Hub infos w/o zxid should be same", hubInfo2, parsedHubInfo2);

        // parse empty string
        try {
            HubInfo.parse("");
            Assert.fail("Should throw InvalidHubInfoException parsing empty string.");
        } catch (HubInfo.InvalidHubInfoException ihie) {
        }

        // parse corrupted hostname
        try {
            HubInfo.parse("localhost,a,b,c");
            Assert.fail("Should throw InvalidHubInfoException parsing corrupted hostname.");
        } catch (HubInfo.InvalidHubInfoException ihie) {
        }

        // parse corrupted string
        try {
            HubInfo.parse("hostname: localhost:9086:9087");
            Assert.fail("Should throw InvalidHubInfoException parsing corrupted string.");
        } catch (HubInfo.InvalidHubInfoException ihie) {
        }
    }
}
