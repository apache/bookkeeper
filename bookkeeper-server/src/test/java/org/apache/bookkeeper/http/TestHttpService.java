/**
 *
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
 *
 */
package org.apache.bookkeeper.http;

import java.util.Map;

import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestHttpService extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterTestCase.class);

    private BKHttpServiceProvider bkHttpServiceProvider;

    public TestHttpService() {
        super(0);
        this.bkHttpServiceProvider = new BKHttpServiceProvider.Builder()
            .setServerConfiguration(baseConf)
            .build();
    }

    @Test
    public void testHeartbeatService() throws Exception {
        // test heartbeat service
        HttpService heartbeatService = bkHttpServiceProvider.provideHeartbeatService();
        HttpServiceResponse response = heartbeatService.handle(null);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals("OK\n", response.getBody());
    }

    @Test
    public void testConfigServiceGet() throws Exception {
        // test config service
        String testProperty = "TEST_PROPERTY";
        String testValue = "TEST_VALUE";
        baseConf.setProperty(testProperty, testValue);
        HttpService configService = bkHttpServiceProvider.provideConfigurationService();
        HttpServiceRequest getRequest = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response = configService.handle(getRequest);
        Map configMap = JsonUtil.fromJson(
            response.getBody(),
            Map.class
        );
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals(testValue, configMap.get(testProperty));
    }

    @Test
    public void testConfigServicePost() throws Exception {
        // test config service
        HttpService configService = bkHttpServiceProvider.provideConfigurationService();
        // properties to be set
        String postBody = "{\"TEST_PROPERTY1\": \"TEST_VALUE1\", \"TEST_PROPERTY2\": 2,  \"TEST_PROPERTY3\": true }";

        // null body, should return NOT_FOUND
        HttpServiceRequest postRequest1 = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse postResponse1 = configService.handle(postRequest1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), postResponse1.getStatusCode());

        // Method DELETE, should return NOT_FOUND
        HttpServiceRequest postRequest2 = new HttpServiceRequest(postBody, HttpServer.Method.DELETE, null);
        HttpServiceResponse postResponse2 = configService.handle(postRequest2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), postResponse2.getStatusCode());

        // Normal POST, should success, then verify using get method
        HttpServiceRequest postRequest3 = new HttpServiceRequest(postBody, HttpServer.Method.POST, null);
        HttpServiceResponse postResponse3 = configService.handle(postRequest3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), postResponse3.getStatusCode());

        // Get all the config
        HttpServiceRequest getRequest = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response = configService.handle(getRequest);
        Map configMap = JsonUtil.fromJson(
          response.getBody(),
          Map.class
        );

        // verify response code
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        // verify response body
        assertEquals("TEST_VALUE1", configMap.get("TEST_PROPERTY1"));
        assertEquals("2", configMap.get("TEST_PROPERTY2"));
        assertEquals("true", configMap.get("TEST_PROPERTY3"));
    }

    @Test
    public void testListBookiesService() throws Exception {
        // test config service
        HttpService listBookiesService = bkHttpServiceProvider.provideListBookiesService();

        // null parameters, should print rw bookies, without hostname
        HttpServiceRequest postRequest1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        // TODO: add test zkutil zkserver into config
        HttpServiceResponse postResponse1 = listBookiesService.handle(postRequest1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), postResponse1.getStatusCode());
    }
}
