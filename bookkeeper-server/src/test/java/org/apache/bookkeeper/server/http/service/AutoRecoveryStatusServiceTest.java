/*
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

package org.apache.bookkeeper.server.http.service;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link AutoRecoveryStatusService}.
 */
public class AutoRecoveryStatusServiceTest extends BookKeeperClusterTestCase {
    private final ObjectMapper mapper = new ObjectMapper();
    private AutoRecoveryStatusService autoRecoveryStatusService;
    public AutoRecoveryStatusServiceTest() {
        super(1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        autoRecoveryStatusService = new AutoRecoveryStatusService(baseConf);
    }

    @Test
    public void testGetStatus() throws Exception {
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(Boolean.TRUE, json.get("enabled").asBoolean());
    }

    @Test
    public void testEnableStatus() throws Exception {
        Map<String, String> params = ImmutableMap.of("enabled", "true");
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.PUT, params);
        HttpServiceResponse response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(Boolean.TRUE, json.get("enabled").asBoolean());

        request = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        json = mapper.readTree(response.getBody());
        assertEquals(Boolean.TRUE, json.get("enabled").asBoolean());
    }

    @Test
    public void testDisableStatus() throws Exception {
        Map<String, String> params = ImmutableMap.of("enabled", "false");
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.PUT, params);
        HttpServiceResponse response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(Boolean.FALSE, json.get("enabled").asBoolean());

        request = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        json = mapper.readTree(response.getBody());
        assertEquals(Boolean.FALSE, json.get("enabled").asBoolean());
    }

    @Test
    public void testInvalidParams() throws Exception {
        Map<String, String> params = ImmutableMap.of("enable", "false");
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.PUT, params);
        HttpServiceResponse response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response.getStatusCode());
    }

    @Test
    public void testInvalidMethod() throws Exception {
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse response = autoRecoveryStatusService.handle(request);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response.getStatusCode());
    }
}
