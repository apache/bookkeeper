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
package org.apache.bookkeeper.server.http;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.server.http.service.BookieInfoService;
import org.apache.bookkeeper.server.http.service.BookieSanityService;
import org.apache.bookkeeper.server.http.service.BookieSanityService.BookieSanity;
import org.apache.bookkeeper.server.http.service.BookieStateReadOnlyService.ReadOnlyState;
import org.apache.bookkeeper.server.http.service.BookieStateService.BookieState;
import org.apache.bookkeeper.server.http.service.ClusterInfoService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the HTTP service.
 */
public class TestHttpService extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestHttpService.class);

    private BKHttpServiceProvider bkHttpServiceProvider;
    private static final int numberOfBookies = 6;

    public TestHttpService() {
        super(numberOfBookies);
        try {
            File tmpDir = tmpDirs.createNew("bookie_http", "test");
            baseConf.setJournalDirName(tmpDir.getPath())
              .setLedgerDirNames(
                new String[]{tmpDir.getPath()});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        baseClientConf.setStoreSystemtimeAsLedgerCreationTime(true);

        MetadataBookieDriver metadataDriver = BookieResources.createMetadataDriver(
                baseConf, NullStatsLogger.INSTANCE);

        this.bkHttpServiceProvider = new BKHttpServiceProvider.Builder()
            .setBookieServer(serverByIndex(numberOfBookies - 1))
            .setServerConfiguration(baseConf)
            .setLedgerManagerFactory(metadataDriver.getLedgerManagerFactory())
            .build();
    }

    @Test
    public void testHeartbeatService() throws Exception {
        // test heartbeat service
        HttpEndpointService heartbeatService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.HEARTBEAT);
        HttpServiceResponse response = heartbeatService.handle(null);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
        assertEquals("OK\n", response.getBody());
    }

    @Test
    public void testConfigServiceGet() throws Exception {
        try {
            // test config service
            String testProperty = "TEST_PROPERTY";
            String testValue = "TEST_VALUE";
            baseConf.setProperty(testProperty, testValue);
            HttpEndpointService configService = bkHttpServiceProvider
              .provideHttpEndpointService(HttpServer.ApiType.SERVER_CONFIG);
            HttpServiceRequest getRequest = new HttpServiceRequest(null, HttpServer.Method.GET, null);
            HttpServiceResponse response = configService.handle(getRequest);
            Map configMap = JsonUtil.fromJson(
              response.getBody(),
              Map.class
            );
            assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());
            assertEquals(testValue, configMap.get(testProperty));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConfigServicePut() throws Exception {
        // test config service
        HttpEndpointService configService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.SERVER_CONFIG);
        // properties to be set
        String putBody = "{\"TEST_PROPERTY1\": \"TEST_VALUE1\", \"TEST_PROPERTY2\": 2,  \"TEST_PROPERTY3\": true }";

        // null body, should return NOT_FOUND
        HttpServiceRequest putRequest1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse putResponse1 = configService.handle(putRequest1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), putResponse1.getStatusCode());

        // Method DELETE, should return NOT_FOUND
        HttpServiceRequest putRequest2 = new HttpServiceRequest(putBody, HttpServer.Method.DELETE, null);
        HttpServiceResponse putResponse2 = configService.handle(putRequest2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), putResponse2.getStatusCode());

        // Normal PUT, should success, then verify using get method
        HttpServiceRequest putRequest3 = new HttpServiceRequest(putBody, HttpServer.Method.PUT, null);
        HttpServiceResponse putResponse3 = configService.handle(putRequest3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), putResponse3.getStatusCode());

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
        HttpEndpointService listBookiesService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_BOOKIES);

        //1,  null parameters, should print rw bookies, without hostname
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = listBookiesService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        // get response , expected get 3 bookies, and without hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response1.getBody(), HashMap.class);
        assertEquals(numberOfBookies, respBody.size());
        for (int i = 0; i < numberOfBookies; i++) {
            assertEquals(true, respBody.containsKey(getBookie(i).toString()));
            assertEquals(null, respBody.get(getBookie(i).toString()));
        }

        //2, parameter: type=rw&print_hostnames=true, should print rw bookies with hostname
        HashMap<String, String> params = Maps.newHashMap();
        params.put("type", "rw");
        params.put("print_hostnames", "true");
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = listBookiesService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        // get response , expected get numberOfBookies bookies, and with hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody2 = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(numberOfBookies, respBody2.size());
        for (int i = 0; i < numberOfBookies; i++) {
            assertEquals(true, respBody2.containsKey(getBookie(i).toString()));
            assertNotNull(respBody2.get(getBookie(i).toString()));
        }

        //3, parameter: type=ro&print_hostnames=true, should print ro bookies with hostname
        // turn bookie 1 into ro, get it
        setBookieToReadOnly(getBookie(1));
        Thread.sleep(200);
        HashMap<String, String> params3 = Maps.newHashMap();
        params3.put("type", "ro");
        params3.put("print_hostnames", "true");
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, params3);
        HttpServiceResponse response3 = listBookiesService.handle(request3);
        //LOG.info("Turn 1 bookies into RO, should get it in this request");
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        // get response , expected get 1 ro bookies, and with hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody3 = JsonUtil.fromJson(response3.getBody(), HashMap.class);
        assertEquals(1, respBody3.size());
        assertEquals(true, respBody3.containsKey(getBookie(1).toString()));

        // get other 5 rw bookies.
        HashMap<String, String> params4 = Maps.newHashMap();
        params4.put("type", "rw");
        params4.put("print_hostnames", "true");
        HttpServiceRequest request4 = new HttpServiceRequest(null, HttpServer.Method.GET, params4);
        HttpServiceResponse response4 = listBookiesService.handle(request4);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response4.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody4 = JsonUtil.fromJson(response4.getBody(), HashMap.class);
        assertEquals(5, respBody4.size());
        assertEquals(true, respBody4.containsKey(getBookie(2).toString()));
    }

    /**
     * Create ledgers, then test ListLedgerService.
     */
    @Test
    public void testListLedgerService() throws Exception {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 430;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "password".getBytes());
        }

        HttpEndpointService listLedgerService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_LEDGER);

        //1,  null parameters, should print ledger ids, without metadata
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = listLedgerService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        // get response , expected get all ledgers, and without metadata
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> respBody = JsonUtil.fromJson(response1.getBody(), LinkedHashMap.class);
        assertEquals(numLedgers, respBody.size());
        for (int i = 0; i < numLedgers; i++) {
            assertEquals(true, respBody.containsKey(Long.valueOf(lh[i].getId()).toString()));
            assertEquals(null, respBody.get(Long.valueOf(lh[i].getId()).toString()));
        }

        //2, parameter: print_metadata=true, should print ledger ids, with metadata
        HashMap<String, String> params = Maps.newHashMap();
        params.put("print_metadata", "true");
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = listLedgerService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        // get response, expected get all ledgers, and with metadata
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> respBody2 = JsonUtil.fromJson(response2.getBody(), LinkedHashMap.class);
        assertEquals(numLedgers, respBody2.size());
        for (int i = 0; i < numLedgers; i++) {
            assertEquals(true, respBody2.containsKey(Long.valueOf(lh[i].getId()).toString()));
            assertNotNull(respBody2.get(Long.valueOf(lh[i].getId()).toString()));
        }

        //3, parameter: print_metadata=true&page=5,
        // since each page contains 100 ledgers, page=5 should print ledger ids, with metadata for(400--430)
        HashMap<String, String> params3 = Maps.newHashMap();
        params3.put("print_metadata", "true");
        params3.put("page", "5");

        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, params3);
        HttpServiceResponse response3 = listLedgerService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        // get response, expected get 4 ledgers, and with metadata
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> respBody3 = JsonUtil.fromJson(response3.getBody(), LinkedHashMap.class);
        assertEquals(31, respBody3.size());
        for (int i = 400; i < 430; i++) {
            assertEquals(true, respBody3.containsKey(Long.valueOf(lh[i].getId()).toString()));
            assertNotNull(respBody3.get(Long.valueOf(lh[i].getId()).toString()));
        }
    }

    /**
     * Create ledgers, then test Delete Ledger service.
     */
    @Test
    public void testDeleteLedgerService() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }

        HttpEndpointService deleteLedgerService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.DELETE_LEDGER);

        //1,  null parameters of GET, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = deleteLedgerService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  null parameters of DELETE, should return NOT_FOUND
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.DELETE, null);
        HttpServiceResponse response2 = deleteLedgerService.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());

        //3,  delete first ledger, should return OK, and should only get 3 ledgers after delete.
        HashMap<String, String> params = Maps.newHashMap();
        Long ledgerId = lh[0].getId();
        params.put("ledger_id", ledgerId.toString());
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.DELETE, params);
        HttpServiceResponse response3 = deleteLedgerService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        // use list Ledger to verify left 3 ledger
        HttpEndpointService listLedgerService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_LEDGER);
        HttpServiceRequest request4 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response4 = listLedgerService.handle(request4);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response4.getStatusCode());
        // get response , expected get 3 ledgers
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> respBody = JsonUtil.fromJson(response4.getBody(), LinkedHashMap.class);
        assertEquals(3, respBody.size());
    }

    @Test
    public void testGetLedgerMetaService() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "password".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }
        HttpEndpointService getLedgerMetaService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.GET_LEDGER_META);

        //1,  null parameters of GET, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = getLedgerMetaService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  parameters for GET first ledger, should return OK, and contains metadata
        HashMap<String, String> params = Maps.newHashMap();
        Long ledgerId = lh[0].getId();
        params.put("ledger_id", ledgerId.toString());
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = getLedgerMetaService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, Object> respBody = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(1, respBody.size());
        @SuppressWarnings("unchecked")
        HashMap<String, Object> expected = JsonUtil.fromJson(JsonUtil.toJson(lh[0].getLedgerMetadata()), HashMap.class);
        @SuppressWarnings("unchecked")
        HashMap<String, Object> actual = (HashMap<String, Object>) respBody.get(ledgerId.toString());

        // verify LedgerMetadata content is equal
        assertTrue(Maps.difference(expected, actual).areEqual());
    }

    @Test
    public void testReadLedgerEntryService() throws Exception {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 1;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }
        HttpEndpointService readLedgerEntryService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.READ_LEDGER_ENTRY);

        //1,  null parameters of GET, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = readLedgerEntryService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  parameters for GET first ledger, should return OK
        // no start/end entry id, so return all the 100 entries.
        HashMap<String, String> params = Maps.newHashMap();
        Long ledgerId = lh[0].getId();
        params.put("ledger_id", ledgerId.toString());
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = readLedgerEntryService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        // default return all the entries. so should have 100 entries return
        assertEquals(100, respBody.size());

        //2,  parameters for GET first ledger, should return OK
        // start_entry_id=1, end_entry_id=77, so return 77 entries.
        HashMap<String, String> params3 = Maps.newHashMap();
        params3.put("ledger_id", ledgerId.toString());
        params3.put("start_entry_id", "1");
        params3.put("end_entry_id", "77");
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, params3);
        HttpServiceResponse response3 = readLedgerEntryService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody3 = JsonUtil.fromJson(response3.getBody(), HashMap.class);
        assertEquals(77, respBody3.size());
        // Verify the entry content that we got.
        assertTrue(respBody3.get("17").equals(content));
    }

    @Test
    public void testListBookieInfoService() throws Exception {
        HttpEndpointService listBookieInfoService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_BOOKIE_INFO);

        //1,  PUT method, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = listBookieInfoService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2, GET method, expected get 6 bookies info and the cluster total info
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = listBookieInfoService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> respBody = JsonUtil.fromJson(response2.getBody(), LinkedHashMap.class);
        assertEquals(numberOfBookies + 1, respBody.size());
        for (int i = 0; i < numberOfBookies; i++) {
            assertEquals(true, respBody.containsKey(getBookie(i).toString()));
        }
    }

    @Test
    public void testGetLastLogMarkService() throws Exception {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }

        HttpEndpointService getLastLogMarkService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LAST_LOG_MARK);

        //1,  null parameters of PUT, should fail
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = getLastLogMarkService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  null parameters of GET, should return 1 file
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = getLastLogMarkService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(1, respBody.size());
    }

    @Test
    public void testListDiskFilesService() throws Exception {
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }

        HttpEndpointService listDiskFileService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_DISK_FILE);

        //1,  null parameters of GET, should return 3 kind of files: journal, entrylog and index files
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = listDiskFileService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response1.getBody(), HashMap.class);
        assertEquals(3, respBody.size());

        //2,  parameters of GET journal file, should return journal files
        HashMap<String, String> params = Maps.newHashMap();
        params.put("file_type", "journal");
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = listDiskFileService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody2 = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(1, respBody2.size());
    }

    @Test
    public void testRecoveryBookieService() throws Exception {
        HttpEndpointService recoveryBookieService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.RECOVERY_BOOKIE);

        //1,  null body of GET, should return error
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = recoveryBookieService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  null body of PUT, should return error
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response2 = recoveryBookieService.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());

        //3, body with bookie_src, bookie_dest and delete_cookie of PUT, should success.
        String bookieSrc = getBookie(0).toString();
        String putBody3 = "{\"bookie_src\": [ \"" + bookieSrc + "\" ],"
          + "\"delete_cookie\": false }";
        HttpServiceRequest request3 = new HttpServiceRequest(putBody3, HttpServer.Method.PUT, null);
        HttpServiceResponse response3 = recoveryBookieService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());

        //5, body with bookie_src of PUT, should success.
        String putBody5 = "{\"bookie_src\": [ \"" + bookieSrc + "\" ] }";
        HttpServiceRequest request5 = new HttpServiceRequest(putBody5, HttpServer.Method.PUT, null);
        HttpServiceResponse response5 = recoveryBookieService.handle(request5);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response5.getStatusCode());
    }

    AuditorElector auditorElector;
    private Future<?> startAuditorElector() throws Exception {
        String addr = addressByIndex(0).toString();
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAuditorPeriodicBookieCheckInterval(1);
        conf.setMetadataServiceUri("zk://" + zkUtil.getZooKeeperConnectString() + "/ledgers");
        auditorElector = new AuditorElector(addr, conf);
        return auditorElector.start();
    }

    private void stopAuditorElector() throws Exception {
        auditorElector.shutdown();
    }

    @Test
    public void testTriggerAuditService() throws Exception {
        startAuditorElector();

        HttpEndpointService triggerAuditService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.TRIGGER_AUDIT);

        //1,  GET, should return error
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = triggerAuditService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  PUT, should success.
        killBookie(1);
        Thread.sleep(500);
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response2 = triggerAuditService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        stopAuditorElector();
    }

    @Test
    public void testWhoIsAuditorService() throws Exception {
        // start the auditor elector and wait until auditor finishes election.
        startAuditorElector().get();

        HttpEndpointService whoIsAuditorService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.WHO_IS_AUDITOR);

        //1,  GET, should return success
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = whoIsAuditorService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        LOG.info(response1.getBody());
        stopAuditorElector();
    }

    @Test
    public void testListUnderReplicatedLedgerService() throws Exception {
        runFunctionWithLedgerManagerFactory(baseConf, mFactory -> {
            try {
                testListUnderReplicatedLedgerService(mFactory);
            } catch (Exception e) {
                LOG.info("Exception in test", e);
                throw new UncheckedExecutionException(e.getMessage(), e.getCause());
            }
            return null;
        });
    }

    private void testListUnderReplicatedLedgerService(LedgerManagerFactory mFactory) throws Exception {
        startAuditorElector();

        HttpEndpointService listUnderReplicatedLedgerService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LIST_UNDER_REPLICATED_LEDGER);

        //1,  PUT, should return error, because only support GET.
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = listUnderReplicatedLedgerService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  GET, should return success.
        // first put ledger into rereplicate. then use api to list ur ledger.
        @Cleanup LedgerManager ledgerManager = mFactory.newLedgerManager();
        @Cleanup final LedgerUnderreplicationManager underReplicationManager =
            mFactory.newLedgerUnderreplicationManager();

        // 192.0.2.0/24 is reserved TEST-NET range
        LedgerMetadataBuilder metadata = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
            .newEnsembleEntry(0L, Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1000).toBookieId(),
                                      getBookie(0),
                                      getBookie(1)));
        ClientUtil.setupLedger(ledgerManager, 1L, metadata);

        // wait for up to two minutes to complete.
        // if the metadata was created just before checkAllLedgers ran, then we need to wait for the timeout
        long underReplicatedLedger = -1;
        for (int i = 0; i < 120; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                LOG.info("Underreplicated ledgers found, breaking out of loop");
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(underReplicatedLedger != -1);

        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = listUnderReplicatedLedgerService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        stopAuditorElector();
    }

    @Test
    public void testLostBookieRecoveryDelayService() throws Exception {
        HttpEndpointService lostBookieRecoveryDelayService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.LOST_BOOKIE_RECOVERY_DELAY);

        //1,  PUT with null, should return error, because should contains {"delay_seconds": <delay_seconds>}.
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = lostBookieRecoveryDelayService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  GET, should meet exception when get delay seconds
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = lostBookieRecoveryDelayService.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());

        //3, PUT, with body, should success
        String putBody3 = "{\"delay_seconds\": 17 }";
        HttpServiceRequest request3 = new HttpServiceRequest(putBody3, HttpServer.Method.PUT, null);
        HttpServiceResponse response3 = lostBookieRecoveryDelayService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
    }

    @Test
    public void testDecommissionService() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        startAuditorElector();

        HttpEndpointService decommissionService = bkHttpServiceProvider
          .provideHttpEndpointService(HttpServer.ApiType.DECOMMISSION);

        //1,  PUT with null, should return error, because should contains {"bookie_src": <bookie_address>}.
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = decommissionService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  GET, should fail for not support get
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = decommissionService.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());

        //3, PUT, with body, should success.
        String putBody3 = "{\"bookie_src\": \"" + getBookie(1).toString() + "\"}";
        HttpServiceRequest request3 = new HttpServiceRequest(putBody3, HttpServer.Method.PUT, null);
        // after bookie kill, request should success
        killBookie(1);
        HttpServiceResponse response3 = decommissionService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        stopAuditorElector();
    }

    @Test
    public void testTriggerGCService() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "Apache BookKeeper is cool!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }
        HttpEndpointService triggerGCService = bkHttpServiceProvider
            .provideHttpEndpointService(HttpServer.ApiType.GC);

        //1,  GET, should return OK
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = triggerGCService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        assertTrue(response1.getBody().contains("\"is_in_force_gc\" : \"false\""));

        //2, PUT, should return OK
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response2 = triggerGCService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
    }

    @Test
    public void testGCDetailsService() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        int numLedgers = 4;
        int numMsgs = 100;
        LedgerHandle[] lh = new LedgerHandle[numLedgers];
        // create ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i] = bkc.createLedger(digestType, "".getBytes());
        }
        String content = "This is test for GC details service!";
        // add entries
        for (int i = 0; i < numMsgs; i++) {
            for (int j = 0; j < numLedgers; j++) {
                lh[j].addEntry(content.getBytes());
            }
        }
        // close ledgers
        for (int i = 0; i < numLedgers; i++) {
            lh[i].close();
        }
        HttpEndpointService gcDetailsService = bkHttpServiceProvider
            .provideHttpEndpointService(HttpServer.ApiType.GC_DETAILS);

        // force trigger a GC
        HttpEndpointService triggerGCService = bkHttpServiceProvider
            .provideHttpEndpointService(HttpServer.ApiType.GC);
        HttpServiceRequest request0 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response0 = triggerGCService.handle(request0);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response0.getStatusCode());

        //1,  GET, should return OK
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = gcDetailsService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        LOG.info("Get response: {}", response1.getBody());

        //2, PUT, should return NOT_FOUND
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response3 = gcDetailsService.handle(request3);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response3.getStatusCode());
    }

    @Test
    public void testGetBookieState() throws Exception {
        HttpEndpointService bookieStateServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_STATE);

        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = bookieStateServer.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());

        BookieState bs = JsonUtil.fromJson(response1.getBody(), BookieState.class);
        assertEquals(true, bs.isRunning());
        assertEquals(false, bs.isReadOnly());
        assertEquals(true, bs.isAvailableForHighPriorityWrites());
        assertEquals(false, bs.isShuttingDown());
    }

    @Test
    public void testGetBookieSanity() throws Exception {
        HttpEndpointService bookieStateServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_SANITY);

        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        ServerConfiguration conf = servers.get(0).getConfiguration();
        BookieSanityService service = new BookieSanityService(conf);
        HttpServiceResponse response1 = service.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        // run multiple iteration to validate any server side throttling doesn't
        // fail sequential requests.
        for (int i = 0; i < 3; i++) {
            BookieSanity bs = JsonUtil.fromJson(response1.getBody(), BookieSanity.class);
            assertEquals(true, bs.isPassed());
            assertEquals(false, bs.isReadOnly());
        }
        HttpServiceResponse response2 = bookieStateServer.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
    }

    @Test
    public void testGetBookieIsReady() throws Exception {
        HttpEndpointService bookieStateServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_IS_READY);

        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = bookieStateServer.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        assertEquals("OK", response1.getBody());

        // Try using POST instead of GET
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse response2 = bookieStateServer.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());

        // Simulate bookies shutting down
        for (int i = 0; i < bookieCount(); i++) {
            serverByIndex(i).getBookie().getStateManager().forceToShuttingDown();
        }
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response3 = bookieStateServer.handle(request3);
        assertEquals(HttpServer.StatusCode.SERVICE_UNAVAILABLE.getValue(), response3.getStatusCode());
    }

    @Test
    public void testGetBookieInfo() throws Exception {
        HttpEndpointService bookieStateServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_INFO);

        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = bookieStateServer.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        LOG.info("Get response: {}", response1.getBody());

        BookieInfoService.BookieInfo bs = JsonUtil.fromJson(response1.getBody(), BookieInfoService.BookieInfo.class);
        assertTrue(bs.getFreeSpace() > 0);
        assertTrue(bs.getTotalSpace() > 0);

        // Try using POST instead of GET
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse response2 = bookieStateServer.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());
    }

    @Test
    public void testGetClusterInfo() throws Exception {
        HttpEndpointService clusterInfoServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.CLUSTER_INFO);

        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = clusterInfoServer.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        LOG.info("Get response: {}", response1.getBody());

        ClusterInfoService.ClusterInfo info = JsonUtil.fromJson(response1.getBody(),
                ClusterInfoService.ClusterInfo.class);
        assertFalse(info.isAuditorElected());
        assertTrue(info.getAuditorId().length() == 0);
        assertFalse(info.isClusterUnderReplicated());
        assertTrue(info.isLedgerReplicationEnabled());
        assertTrue(info.getTotalBookiesCount() > 0);
        assertTrue(info.getWritableBookiesCount() > 0);
        assertTrue(info.getReadonlyBookiesCount() == 0);
        assertTrue(info.getUnavailableBookiesCount() == 0);
        assertTrue(info.getTotalBookiesCount() == info.getWritableBookiesCount());

        // Try using POST instead of GET
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse response2 = clusterInfoServer.handle(request2);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response2.getStatusCode());
    }

    @Test
    public void testBookieReadOnlyState() throws Exception {
        HttpEndpointService bookieStateServer = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_STATE);
        HttpEndpointService bookieReadOnlyService = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.BOOKIE_STATE_READONLY);

        // responses from both endpoints should indicate the bookie is not read only
        HttpServiceRequest request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response = bookieStateServer.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());

        BookieState bs = JsonUtil.fromJson(response.getBody(), BookieState.class);
        assertTrue(bs.isRunning());
        assertFalse(bs.isReadOnly());
        assertTrue(bs.isAvailableForHighPriorityWrites());
        assertFalse(bs.isShuttingDown());

        request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        response = bookieReadOnlyService.handle(request);
        ReadOnlyState readOnlyState = JsonUtil.fromJson(response.getBody(), ReadOnlyState.class);
        assertFalse(readOnlyState.isReadOnly());

        // update the state to read only
        request = new HttpServiceRequest(JsonUtil.toJson(new ReadOnlyState(true)), HttpServer.Method.PUT,  null);
        response = bookieReadOnlyService.handle(request);
        readOnlyState = JsonUtil.fromJson(response.getBody(), ReadOnlyState.class);
        assertTrue(readOnlyState.isReadOnly());

        // responses from both endpoints should indicate the bookie is read only
        request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        response = bookieStateServer.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());

        bs = JsonUtil.fromJson(response.getBody(), BookieState.class);
        assertTrue(bs.isRunning());
        assertTrue(bs.isReadOnly());
        assertTrue(bs.isAvailableForHighPriorityWrites());
        assertFalse(bs.isShuttingDown());

        request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        response = bookieReadOnlyService.handle(request);
        readOnlyState = JsonUtil.fromJson(response.getBody(), ReadOnlyState.class);
        assertTrue(readOnlyState.isReadOnly());

        // should be able to update the state to writable again
        request = new HttpServiceRequest(JsonUtil.toJson(new ReadOnlyState(false)), HttpServer.Method.PUT,  null);
        response = bookieReadOnlyService.handle(request);
        readOnlyState = JsonUtil.fromJson(response.getBody(), ReadOnlyState.class);
        assertFalse(readOnlyState.isReadOnly());

        // responses from both endpoints should indicate the bookie is writable
        request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        response = bookieStateServer.handle(request);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response.getStatusCode());

        bs = JsonUtil.fromJson(response.getBody(), BookieState.class);
        assertTrue(bs.isRunning());
        assertFalse(bs.isReadOnly());
        assertTrue(bs.isAvailableForHighPriorityWrites());
        assertFalse(bs.isShuttingDown());

        request = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        response = bookieReadOnlyService.handle(request);
        readOnlyState = JsonUtil.fromJson(response.getBody(), ReadOnlyState.class);
        assertFalse(readOnlyState.isReadOnly());

        //forceReadonly to writable
        baseConf.setForceReadOnlyBookie(true);
        baseConf.setReadOnlyModeEnabled(true);
        restartBookies();
        request = new HttpServiceRequest(JsonUtil.toJson(new ReadOnlyState(false)), HttpServer.Method.PUT,  null);
        response = bookieReadOnlyService.handle(request);
        assertEquals(400, response.getStatusCode());
    }

    @Test
    public void testSuspendCompaction() throws Exception {
        HttpEndpointService suspendCompactionService = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.SUSPEND_GC_COMPACTION);

        HttpEndpointService resumeCompactionService = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.RESUME_GC_COMPACTION);

        //1,  PUT with null body, should return error
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = suspendCompactionService.handle(request1);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response1.getStatusCode());

        //2,  PUT with null, should return error, because should contains "suspendMajor" or "suspendMinor"
        String putBody2 = "{}";
        HttpServiceRequest request2 = new HttpServiceRequest(putBody2, HttpServer.Method.PUT, null);
        HttpServiceResponse response2 = suspendCompactionService.handle(request2);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response2.getStatusCode());


        //3,  GET before suspend, should success
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response3 = suspendCompactionService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());

        Map responseMap = JsonUtil.fromJson(
                response3.getBody(),
                Map.class
        );
        assertEquals(responseMap.get("isMajorGcSuspended"), "false");
        assertEquals(responseMap.get("isMinorGcSuspended"), "false");


        //2, PUT, with body, should success
        String putBody4 = "{\"suspendMajor\": true, \"suspendMinor\": true}";
        HttpServiceRequest request4 = new HttpServiceRequest(putBody4, HttpServer.Method.PUT, null);
        HttpServiceResponse response4 = suspendCompactionService.handle(request4);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response4.getStatusCode());

        //3,  GET after suspend, should success
        HttpServiceRequest request5 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response5 = suspendCompactionService.handle(request5);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response5.getStatusCode());

        Map responseMap5 = JsonUtil.fromJson(
                response5.getBody(),
                Map.class
        );
        assertEquals(responseMap5.get("isMajorGcSuspended"), "true");
        assertEquals(responseMap5.get("isMinorGcSuspended"), "true");


        //2, PUT, with body, should success
        String putBody6 = "{\"resumeMajor\": true, \"resumeMinor\": true}";
        HttpServiceRequest request6 = new HttpServiceRequest(putBody6, HttpServer.Method.PUT, null);
        HttpServiceResponse response6 = resumeCompactionService.handle(request6);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response6.getStatusCode());

        //3,  GET after suspend, should success
        HttpServiceRequest request7 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response7 = suspendCompactionService.handle(request7);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response7.getStatusCode());

        Map responseMap7 = JsonUtil.fromJson(
                response7.getBody(),
                Map.class
        );
        assertEquals(responseMap7.get("isMajorGcSuspended"), "false");
        assertEquals(responseMap7.get("isMinorGcSuspended"), "false");
    }

    @Test
    public void testTriggerEntryLocationCompactService() throws Exception {
        BookieServer bookieServer = serverByIndex(numberOfBookies - 1);
        LedgerStorage spyLedgerStorage = spy(bookieServer.getBookie().getLedgerStorage());
        List<String> dbLocationPath = Lists.newArrayList("/data1/bookkeeper/ledgers/current/locations",
                "/data2/bookkeeper/ledgers/current/locations");
        when(spyLedgerStorage.getEntryLocationDBPath())
                .thenReturn(dbLocationPath);

        HashMap<String, Boolean> statusMap = Maps.newHashMap();
        statusMap.put("/data1/bookkeeper/ledgers/current/locations", false);
        statusMap.put("/data2/bookkeeper/ledgers/current/locations", true);
        when(spyLedgerStorage.isEntryLocationCompacting(dbLocationPath))
                .thenReturn(statusMap);

        Field ledgerStorageField = bookieServer.getBookie().getClass().getDeclaredField("ledgerStorage");
        ledgerStorageField.setAccessible(true);
        ledgerStorageField.set(bookieServer.getBookie(), spyLedgerStorage);

        HttpEndpointService triggerEntryLocationCompactService = bkHttpServiceProvider
                .provideHttpEndpointService(HttpServer.ApiType.TRIGGER_ENTRY_LOCATION_COMPACT);

        // 1. Put
        // 1.1 Trigger all entry location rocksDB compact, should return OK
        HttpServiceRequest request1 = new HttpServiceRequest("{\"entryLocationRocksDBCompact\":true}",
                HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = triggerEntryLocationCompactService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        LOG.info("Get response: {}", response1.getBody());

        // 1.2 Specified trigger entry location rocksDB compact, should return OK
        String body2 = "{\"entryLocationRocksDBCompact\":true,\"entryLocations\""
               + ":\"/data1/bookkeeper/ledgers/current/locations\"}";
        HttpServiceRequest request2 = new HttpServiceRequest(body2, HttpServer.Method.PUT, null);
        HttpServiceResponse response2 = triggerEntryLocationCompactService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        LOG.info("Get response: {}", response2.getBody());
        assertTrue(response2.getBody().contains("Triggered entry Location RocksDB"));

        // 1.3 Specified invalid entry location rocksDB compact, should return BAD_REQUEST
        String body3 = "{\"entryLocationRocksDBCompact\":true,\"entryLocations\""
                + ":\"/invalid1/locations,/data2/bookkeeper/ledgers/current/locations\"}";
        HttpServiceRequest request3 = new HttpServiceRequest(body3, HttpServer.Method.PUT, null);
        HttpServiceResponse response3 = triggerEntryLocationCompactService.handle(request3);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response3.getStatusCode());
        LOG.info("Get response: {}", response3.getBody());
        assertTrue(response3.getBody().contains("is invalid"));

        // 1.4 Some rocksDB is running compact, should return OK
        String body4 = "{\"entryLocationRocksDBCompact\":true,\"entryLocations\""
                + ":\"/data1/bookkeeper/ledgers/current/locations,/data2/bookkeeper/ledgers/current/locations\"}";
        HttpServiceRequest request4 = new HttpServiceRequest(body4, HttpServer.Method.PUT, null);
        HttpServiceResponse response4 = triggerEntryLocationCompactService.handle(request4);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response4.getStatusCode());
        LOG.info("Get response: {}", response4.getBody());

        // 1.5 Put, empty body, should return BAD_REQUEST
        HttpServiceRequest request5 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response5 = triggerEntryLocationCompactService.handle(request5);
        assertEquals(HttpServer.StatusCode.BAD_REQUEST.getValue(), response5.getStatusCode());
        LOG.info("Get response: {}", response5.getBody());

        // 2. GET, should return OK
        HttpServiceRequest request6 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response6 = triggerEntryLocationCompactService.handle(request6);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response6.getStatusCode());
        assertTrue(response6.getBody().contains("\"/data2/bookkeeper/ledgers/current/locations\" : true"));
        assertTrue(response6.getBody().contains("\"/data1/bookkeeper/ledgers/current/locations\" : false"));

        // 3. POST, should return NOT_FOUND
        HttpServiceRequest request7 = new HttpServiceRequest(null, HttpServer.Method.POST, null);
        HttpServiceResponse response7 = triggerEntryLocationCompactService.handle(request7);
        assertEquals(HttpServer.StatusCode.METHOD_NOT_ALLOWED.getValue(), response7.getStatusCode());
    }
}
