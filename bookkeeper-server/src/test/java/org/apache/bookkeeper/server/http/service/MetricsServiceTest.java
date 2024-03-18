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
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.Writer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer.Method;
import org.apache.bookkeeper.http.HttpServer.StatusCode;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.stats.StatsProvider;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link MetricsService}.
 */
public class MetricsServiceTest {

    private StatsProvider mockStatsProvider;
    private MetricsService service;

    @Before
    public void setup() {
        this.mockStatsProvider = mock(StatsProvider.class);
        this.service = new MetricsService(new ServerConfiguration(), mockStatsProvider);
    }

    @Test
    public void testForbiddenMethods() throws Exception {
        HttpServiceRequest request = new HttpServiceRequest().setMethod(Method.PUT);
        HttpServiceResponse response = service.handle(request);
        assertEquals(StatusCode.FORBIDDEN.getValue(), response.getStatusCode());
        assertNull(response.getContentType());
        assertEquals(
            "PUT is forbidden. Should be GET method",
            response.getBody());
    }

    @Test
    public void testNullStatsProvider() throws Exception {
        service = new MetricsService(new ServerConfiguration(), null);
        HttpServiceRequest request = new HttpServiceRequest().setMethod(Method.GET);
        HttpServiceResponse response = service.handle(request);
        assertEquals(StatusCode.INTERNAL_ERROR.getValue(), response.getStatusCode());
        assertNull(response.getContentType());
        assertEquals(
            "Stats provider is not enabled. Please enable it by set statsProviderClass"
                + " on bookie configuration",
            response.getBody());
    }

    @Test
    public void testWriteMetrics() throws Exception {
        String content = "test-metrics";

        doAnswer(invocationOnMock -> {
            Writer writer = invocationOnMock.getArgument(0);
            writer.write(content);
            return null;
        }).when(mockStatsProvider).writeAllMetrics(any(Writer.class));

        HttpServiceRequest request = new HttpServiceRequest().setMethod(Method.GET);
        HttpServiceResponse response = service.handle(request);

        assertEquals(StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals(MetricsService.PROMETHEUS_CONTENT_TYPE_004, response.getContentType());
        assertEquals(content, response.getBody());
    }

    @Test
    public void testWriteMetricsException() throws Exception {
        doThrow(new IOException("write-metrics-exception"))
            .when(mockStatsProvider).writeAllMetrics(any(Writer.class));

        HttpServiceRequest request = new HttpServiceRequest().setMethod(Method.GET);
        HttpServiceResponse response = service.handle(request);

        assertEquals(StatusCode.INTERNAL_ERROR.getValue(), response.getStatusCode());
        assertNull(response.getContentType());
        assertEquals("Exceptions are thrown when exporting metrics : write-metrics-exception",
            response.getBody());
    }

    @Test
    public void testWriteMetricsUnimplemented() throws Exception {
        mockStatsProvider = mock(StatsProvider.class, CALLS_REAL_METHODS);
        service = new MetricsService(new ServerConfiguration(), mockStatsProvider);

        HttpServiceRequest request = new HttpServiceRequest().setMethod(Method.GET);
        HttpServiceResponse response = service.handle(request);

        assertEquals(StatusCode.INTERNAL_ERROR.getValue(), response.getStatusCode());
        assertNull(response.getContentType());
        assertEquals("Currently stats provider doesn't support exporting metrics in http service",
            response.getBody());
    }

}
