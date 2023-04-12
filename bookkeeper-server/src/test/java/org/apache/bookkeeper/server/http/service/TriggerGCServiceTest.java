/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.server.http.service;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

public class TriggerGCServiceTest extends BookKeeperClusterTestCase {

    private static final int numberOfBookies = 1;
    private TriggerGCService triggerGCService;

    public TriggerGCServiceTest() {
        super(numberOfBookies);
    }

    private List<GarbageCollectionStatus> getGCCollectionStats() throws Exception {
        assertEquals(1, bookieCount());
        BookieServer server = serverByIndex(0);
        return server.getBookie().getLedgerStorage().getGarbageCollectionStatus();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        triggerGCService = new TriggerGCService(baseConf, serverByIndex(numBookies - 1));
    }

    @Test
    public void testGCTriggerSuccessWithForceMajorAndMinorCompact() throws Exception {
        String testBody = "{ \"forceMajor\": true, \"forceMinor\": true }";
        HttpServiceRequest request = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        long majorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        HttpServiceResponse httpServiceResponse = triggerGCService.handle(request);
        while (serverByIndex(numBookies - 1).getBookie().getLedgerStorage().isInForceGC()) {
            Thread.sleep(1000);
        }
        long majorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        assertEquals(majorCompactionCounterBeforeTrigger + 1, majorCompactionCounterAfterTrigger);
        assertEquals(minorCompactionCounterBeforeTrigger, minorCompactionCounterAfterTrigger);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpServiceResponse.getStatusCode());
    }

    @Test
    public void testGCTriggerSuccessWithForceMajorCompactOnly() throws Exception {
        String testBody = "{ \"forceMajor\": true, \"forceMinor\": false }";
        HttpServiceRequest request = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        long majorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        HttpServiceResponse httpServiceResponse = triggerGCService.handle(request);
        while (serverByIndex(numBookies - 1).getBookie().getLedgerStorage().isInForceGC()) {
            Thread.sleep(1000);
        }
        long majorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        assertEquals(majorCompactionCounterBeforeTrigger + 1, majorCompactionCounterAfterTrigger);
        assertEquals(minorCompactionCounterBeforeTrigger, minorCompactionCounterAfterTrigger);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpServiceResponse.getStatusCode());
    }

    @Test
    public void testGCTriggerSuccessWithForceMinorCompactOnly() throws Exception {
        String testBody = "{ \"forceMajor\": false, \"forceMinor\": true }";
        HttpServiceRequest request = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        long majorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        HttpServiceResponse httpServiceResponse = triggerGCService.handle(request);
        while (serverByIndex(numBookies - 1).getBookie().getLedgerStorage().isInForceGC()) {
            Thread.sleep(1000);
        }
        long majorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        assertEquals(majorCompactionCounterBeforeTrigger, majorCompactionCounterAfterTrigger);
        assertEquals(minorCompactionCounterBeforeTrigger + 1, minorCompactionCounterAfterTrigger);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpServiceResponse.getStatusCode());
    }

    @Test
    public void testGCTriggerSuccessWithBadRequestBody() throws Exception {
        String testBody = "{ \"forceMajor\": \"true\", \"forceMinor\": [true] }";
        HttpServiceRequest request = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        long majorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterBeforeTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        HttpServiceResponse httpServiceResponse = triggerGCService.handle(request);
        while (serverByIndex(numBookies - 1).getBookie().getLedgerStorage().isInForceGC()) {
            Thread.sleep(1000);
        }
        long majorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMajorCompactionCounter();
        long minorCompactionCounterAfterTrigger = getGCCollectionStats().get(0).getMinorCompactionCounter();
        assertEquals(majorCompactionCounterBeforeTrigger, majorCompactionCounterAfterTrigger);
        assertEquals(minorCompactionCounterBeforeTrigger, minorCompactionCounterAfterTrigger);
        assertEquals(HttpServer.StatusCode.OK.getValue(), httpServiceResponse.getStatusCode());
    }

}
