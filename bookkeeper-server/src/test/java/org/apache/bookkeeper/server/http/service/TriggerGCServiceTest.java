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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit test for {@link TriggerGCService}.
 */
@Slf4j
public class TriggerGCServiceTest {
    private TriggerGCService service;
    private BookieServer mockBookieServer;
    private LedgerStorage mockLedgerStorage;

    @Before
    public void setup() {
        this.mockBookieServer = mock(BookieServer.class, RETURNS_DEEP_STUBS);
        this.mockLedgerStorage = mock(DbLedgerStorage.class);
        when(mockBookieServer.getBookie().getLedgerStorage()).thenReturn(mockLedgerStorage);
        when(mockLedgerStorage.isInForceGC()).thenReturn(false);
        when(mockLedgerStorage.isMajorGcSuspended()).thenReturn(false);
        when(mockLedgerStorage.isMinorGcSuspended()).thenReturn(false);
        this.service = new TriggerGCService(new ServerConfiguration(), mockBookieServer);
    }

    @Test
    public void testHandleRequest() throws Exception {

        // test empty put body
        HttpServiceRequest request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        HttpServiceResponse resp = service.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test invalid put json body
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("test");
        resp = service.handle(request);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), resp.getStatusCode());
        assertEquals("Failed to handle the request, exception: Failed to deserialize Object from Json string",
            resp.getBody());

        // test forceMajor and forceMinor not set
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"test\":1}");
        resp = service.handle(request);
        verify(mockLedgerStorage, times(1)).forceGC(eq(true), eq(true));
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test forceMajor set, but forceMinor not set
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"test\":1,\"forceMajor\":true}");
        resp = service.handle(request);
        verify(mockLedgerStorage, times(2)).forceGC(eq(true), eq(true));
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test forceMajor set, but forceMinor not set
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"test\":1,\"forceMajor\":\"true\"}");
        resp = service.handle(request);
        verify(mockLedgerStorage, times(3)).forceGC(eq(true), eq(true));
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test forceMajor set to false, and forMinor not set
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"test\":1,\"forceMajor\":false}");
        resp = service.handle(request);
        verify(mockLedgerStorage, times(1)).forceGC(eq(false), eq(true));
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test forceMajor not set and forMinor set
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.PUT);
        request.setBody("{\"test\":1,\"forceMinor\":true}");
        resp = service.handle(request);
        verify(mockLedgerStorage, times(4)).forceGC(eq(true), eq(true));
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("\"Triggered GC on BookieServer: " + mockBookieServer.getBookieId() + "\"",
            resp.getBody());

        // test get gc
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.GET);
        resp = service.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), resp.getStatusCode());
        assertEquals("{\n  \"is_in_force_gc\" : \"false\"\n}", resp.getBody());

        // test invalid method type
        request = new HttpServiceRequest();
        request.setMethod(HttpServer.Method.POST);
        resp = service.handle(request);
        assertEquals(HttpServer.StatusCode.METHOD_NOT_ALLOWED.getValue(), resp.getStatusCode());
        assertEquals("Not allowed method. Should be PUT to trigger GC, Or GET to get Force GC state.",
            resp.getBody());
    }

}
