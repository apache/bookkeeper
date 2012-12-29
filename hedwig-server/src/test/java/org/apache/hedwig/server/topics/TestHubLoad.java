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

import org.junit.Test;
import org.junit.Assert;

public class TestHubLoad {

    @Test(timeout=60000)
    public void testParseHubLoad() throws Exception {
        HubLoad hubLoad1 = new HubLoad(9999);

        String strHubLoad1 = hubLoad1.toString();
        HubLoad parsedHubLoad1 = HubLoad.parse(strHubLoad1);
        Assert.assertEquals("Hub load data should be same", hubLoad1, parsedHubLoad1);

        final int numTopics = 9998;
        HubLoad hubLoad2 = new HubLoad(numTopics);
        HubLoad parsedHubLoad2 = HubLoad.parse(numTopics + "");
        Assert.assertEquals("Hub load data not protobuf encoded should be same", hubLoad2, parsedHubLoad2);

        // parse empty string
        try {
            HubLoad.parse("");
            Assert.fail("Should throw InvalidHubLoadException parsing empty string.");
        } catch (HubLoad.InvalidHubLoadException ihie) {
        }

        // parse corrupted numTopics
        try {
            HubLoad.parse("9998_x");
            Assert.fail("Should throw InvalidHubLoadException parsing corrupted hub load data.");
        } catch (HubLoad.InvalidHubLoadException ihie) {
        }

        // parse corrupted string
        try {
            HubLoad.parse("hostname: 9998_x");
            Assert.fail("Should throw InvalidHubLoadException parsing corrupted hub load data.");
        } catch (HubLoad.InvalidHubLoadException ihie) {
        }
    }
}
