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

import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;


public class TriggerLocationCompactServiceTest extends BookKeeperClusterTestCase {

    private static final int numberOfBookies = 1;
    private TriggerLocationCompactService triggerLocationCompactService;

    public TriggerLocationCompactServiceTest() {
        super(numberOfBookies);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        triggerLocationCompactService = new TriggerLocationCompactService(serverByIndex(numBookies - 1));
    }

    @Test
    public void testTriggerLocationCompactServiceWithBadEntryLocationCompactFlag() throws Exception {
        String testBody = "{ \"entryLocationRocksDBCompact\": \"true\" }";
        String expectedOutput = "Not trigger Entry Location RocksDB compact.";
        HttpServiceRequest request = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = triggerLocationCompactService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals(expectedOutput, response.getBody());
    }

}
