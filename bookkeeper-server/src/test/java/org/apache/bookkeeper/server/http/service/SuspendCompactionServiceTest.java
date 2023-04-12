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

import java.util.Map;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

public class SuspendCompactionServiceTest extends BookKeeperClusterTestCase {

    private static final int numberOfBookies = 1;
    private SuspendCompactionService suspendCompactionService;

    public SuspendCompactionServiceTest() {
        super(numberOfBookies);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        suspendCompactionService = new SuspendCompactionService(serverByIndex(numBookies - 1));
    }

    @Test
    public void testSuspendCompactionWithBadMajorAndMinorCompactArgs() throws Exception {
        String testBody = "{ \"suspendMajor\": \"true\", \"suspendMinor\": \"true\" }";
        Map beforeStatusResponseMap = getCompactionStatus();
        assertEquals("false", beforeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("false", beforeStatusResponseMap.get("isMinorGcSuspended"));
        HttpServiceRequest suspendRequest = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = suspendCompactionService.handle(suspendRequest);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response.getStatusCode());

    }

    @Test
    public void testSuspendCompactionWithBadMajorAndGoodMinorCompactArgs() throws Exception {
        String testBody = "{ \"suspendMajor\": \"true\", \"suspendMinor\": true }";
        Map beforeStatusResponseMap = getCompactionStatus();
        assertEquals("false", beforeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("false", beforeStatusResponseMap.get("isMinorGcSuspended"));
        HttpServiceRequest suspendRequest = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = suspendCompactionService.handle(suspendRequest);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        Map afterStatusResponseMap = getCompactionStatus();
        assertEquals("false", afterStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("true", afterStatusResponseMap.get("isMinorGcSuspended"));
    }

    @Test
    public void testSuspendCompactionWithGoodMajorAndBadMinorCompactArgs() throws Exception {
        String testBody = "{ \"suspendMajor\": true, \"suspendMinor\": \"true\" }";
        Map beforeStatusResponseMap = getCompactionStatus();
        assertEquals("false", beforeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("false", beforeStatusResponseMap.get("isMinorGcSuspended"));
        HttpServiceRequest suspendRequest = new HttpServiceRequest(testBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = suspendCompactionService.handle(suspendRequest);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        Map afterStatusResponseMap = getCompactionStatus();
        assertEquals("true", afterStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("false", afterStatusResponseMap.get("isMinorGcSuspended"));
    }

    private Map getCompactionStatus() throws Exception {
        HttpServiceRequest statusRequest = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse statusResponse = suspendCompactionService.handle(statusRequest);
        return JsonUtil.fromJson(statusResponse.getBody(), Map.class);
    }

}
