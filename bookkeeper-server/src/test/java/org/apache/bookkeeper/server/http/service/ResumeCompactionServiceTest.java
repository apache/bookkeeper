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

public class ResumeCompactionServiceTest extends BookKeeperClusterTestCase {

    private static final int numberOfBookies = 1;
    private SuspendCompactionService suspendCompactionService;
    private ResumeCompactionService resumeCompactionService;

    public ResumeCompactionServiceTest() {
        super(numberOfBookies);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        resumeCompactionService = new ResumeCompactionService(serverByIndex(numBookies - 1));
        suspendCompactionService = new SuspendCompactionService(serverByIndex(numBookies - 1));
    }

    @Test
    public void testResumeCompactionWithBadMajorAndMinorCompactArgs() throws Exception {
        String triggerResumeBody = "{ \"resumeMajor\": \"true\", \"resumeMinor\": \"true\" }";
        HttpServiceRequest resumeRequest = new HttpServiceRequest(triggerResumeBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = resumeCompactionService.handle(resumeRequest);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response.getStatusCode());
    }

    @Test
    public void testResumeCompactionWithBadMajorAndGoodMinorCompactArgs() throws Exception {
        String triggerSuspendBody = "{ \"suspendMajor\": true, \"suspendMinor\": true }";
        String triggerResumeBody = "{ \"resumeMajor\": \"true\", \"resumeMinor\": true }";
        HttpServiceRequest suspendRequest = new HttpServiceRequest(triggerSuspendBody, HttpServer.Method.PUT, null);
        suspendCompactionService.handle(suspendRequest);
        Map beforeResumeStatusResponseMap = getCompactionStatus();
        assertEquals("true", beforeResumeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("true", beforeResumeStatusResponseMap.get("isMinorGcSuspended"));
        HttpServiceRequest resumeRequest = new HttpServiceRequest(triggerResumeBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = resumeCompactionService.handle(resumeRequest);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        Map afterResumeStatusResponseMap = getCompactionStatus();
        assertEquals("true", afterResumeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("false", afterResumeStatusResponseMap.get("isMinorGcSuspended"));
    }

    @Test
    public void testResumeCompactionWithGoodMajorAndBadMinorCompactArgs() throws Exception {
        String triggerSuspendBody = "{ \"suspendMajor\": true, \"suspendMinor\": true }";
        String triggerResumeBody = "{ \"resumeMajor\": true, \"resumeMinor\": \"true\" }";
        HttpServiceRequest suspendRequest = new HttpServiceRequest(triggerSuspendBody, HttpServer.Method.PUT, null);
        suspendCompactionService.handle(suspendRequest);
        Map beforeResumeStatusResponseMap = getCompactionStatus();
        assertEquals("true", beforeResumeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("true", beforeResumeStatusResponseMap.get("isMinorGcSuspended"));
        HttpServiceRequest resumeRequest = new HttpServiceRequest(triggerResumeBody, HttpServer.Method.PUT, null);
        HttpServiceResponse response = resumeCompactionService.handle(resumeRequest);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        Map afterResumeStatusResponseMap = getCompactionStatus();
        assertEquals("false", afterResumeStatusResponseMap.get("isMajorGcSuspended"));
        assertEquals("true", afterResumeStatusResponseMap.get("isMinorGcSuspended"));
    }

    private Map getCompactionStatus() throws Exception {
        HttpServiceRequest statusRequest = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse statusResponse = suspendCompactionService.handle(statusRequest);
        return JsonUtil.fromJson(statusResponse.getBody(), Map.class);
    }

}
