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

import org.apache.bookkeeper.http.service.Service;
import org.apache.bookkeeper.http.service.ServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.JsonUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHttpService extends BookKeeperClusterTestCase {

    private BKServiceProvider serviceProvider;

    public TestHttpService() {
        super(0);
        this.serviceProvider = new BKServiceProvider.Builder()
            .setServerConfiguration(baseConf)
            .build();
    }

    @Test
    public void testHeartbeatService() throws Exception {
        // test heartbeat service
        Service heartbeatService = serviceProvider.provideHeartbeatService();
        ServiceResponse response = heartbeatService.handle(null);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals("OK\n", response.getBody());
    }

    @Test
    public void testConfigService() throws Exception {
        // test config service
        String testProperty = "TEST_PROPERTY";
        String testValue = "TEST_VALUE";
        baseConf.setProperty(testProperty, testValue);
        Service configService = serviceProvider.provideConfigurationService();
        ServiceResponse response = configService.handle(null);
        Map configMap = JsonUtil.fromJson(
            response.getBody(),
            Map.class
        );
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals(testValue, configMap.get(testProperty));
    }

}
