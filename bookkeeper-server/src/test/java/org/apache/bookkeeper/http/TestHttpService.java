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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.http.service.HttpService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHttpService extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestHttpService.class);

    private BKHttpServiceProvider bkHttpServiceProvider;

    public TestHttpService() {
        super(3);
        try {
            File tmpDir = createTempDir("bookie_http", "test");
            baseConf.setJournalDirName(tmpDir.getPath())
              .setLedgerDirNames(
                new String[]{tmpDir.getPath()});
            this.bkHttpServiceProvider = new BKHttpServiceProvider.Builder()
              .setServerConfiguration(baseConf)
              .build();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
        HttpService listBookiesService = bkHttpServiceProvider.provideListBookiesService();

        //1,  null parameters, should print rw bookies, without hostname
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = listBookiesService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        // get response , expected get 3 bookies, and without hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response1.getBody(), HashMap.class);
        assertEquals(3, respBody.size());
        for (int i = 0; i < 3; i++) {
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
        // get response , expected get 3 bookies, and with hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody2 = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(3, respBody2.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(true, respBody2.containsKey(getBookie(i).toString()));
            assertNotNull(respBody2.get(getBookie(i).toString()));
        }

        //3, parameter: type=ro&print_hostnames=true, should print ro bookies with hostname
        // turn bookie to ro, get
        for (int i = 0; i < 3; i++) {
            setBookieToReadOnly(getBookie(i));
        }
        LOG.info("turn all 3 bookies into RO");
        HashMap<String, String> params3 = Maps.newHashMap();
        params3.put("type", "ro");
        params3.put("print_hostnames", "true");
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.GET, params3);
        HttpServiceResponse response3 = listBookiesService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        // get response , expected get 3 ro bookies, and with hostname
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody3 = JsonUtil.fromJson(response3.getBody(), HashMap.class);
        assertEquals(3, respBody3.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(true, respBody3.containsKey(getBookie(i).toString()));
            assertNotNull(respBody3.get(getBookie(i).toString()));
        }
    }

    /**
     * create ledgers, then test ListLedgerService
     */
    @Test
    public void testListLedgerService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
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

        HttpService listLedgerService = bkHttpServiceProvider.provideListLedgerService();

        //1,  null parameters, should print ledger ids, without metadata
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = listLedgerService.handle(request1);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response1.getStatusCode());
        // get response , expected get 4 ledgers, and without metadata
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response1.getBody(), HashMap.class);
        assertEquals(4, respBody.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(true, respBody.containsKey(Long.valueOf(lh[i].getId()).toString()));
            assertEquals(null, respBody.get(Long.valueOf(lh[i].getId()).toString()));
        }

        //2, parameter: print_metadata=true, should print ledger ids, with metadata
        HashMap<String, String> params = Maps.newHashMap();
        params.put("print_metadata", "true");
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = listLedgerService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        // get response, expected get 4 ledgers, and with metadata
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody2 = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(4, respBody2.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(true, respBody2.containsKey(Long.valueOf(lh[i].getId()).toString()));
            assertNotNull(respBody2.get(Long.valueOf(lh[i].getId()).toString()));
        }
    }

    /**
     * create ledgers, then test Delete Ledger service
     */
    @Test
    public void testDeleteLedgerService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
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

        HttpService deleteLedgerService = bkHttpServiceProvider.provideDeleteLedgerService();

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
        Long ledgerId = Long.valueOf(lh[0].getId());
        params.put("ledger_id", ledgerId.toString());
        HttpServiceRequest request3 = new HttpServiceRequest(null, HttpServer.Method.DELETE, params);
        HttpServiceResponse response3 = deleteLedgerService.handle(request3);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response3.getStatusCode());
        // use list Ledger to verify left 3 ledger
        HttpService listLedgerService = bkHttpServiceProvider.provideListLedgerService();
        HttpServiceRequest request4 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response4 = listLedgerService.handle(request4);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response4.getStatusCode());
        // get response , expected get 3 ledgers
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response4.getBody(), HashMap.class);
        assertEquals(3, respBody.size());
    }

    @Test
    public void testGetLedgerMetaService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
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
        HttpService getLedgerMetaService = bkHttpServiceProvider.provideGetLedgerMetaService();

        //1,  null parameters of GET, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = getLedgerMetaService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  parameters for GET first ledger, should return OK, and contains metadata
        HashMap<String, String> params = Maps.newHashMap();
        Long ledgerId = Long.valueOf(lh[0].getId());
        params.put("ledger_id", ledgerId.toString());
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, params);
        HttpServiceResponse response2 = getLedgerMetaService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(1, respBody.size());
    }

    @Test
    public void testReadLedgerEntryService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
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
        HttpService readLedgerEntryService = bkHttpServiceProvider.provideReadLedgerEntryService();

        //1,  null parameters of GET, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response1 = readLedgerEntryService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2,  parameters for GET first ledger, should return OK
        // no start/end entry id, so return all the 100 entries.
        HashMap<String, String> params = Maps.newHashMap();
        Long ledgerId = Long.valueOf(lh[0].getId());
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
    }

    @Test
    public void testListBookieInfoService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
        HttpService listBookieInfoService = bkHttpServiceProvider.provideListBookieInfoService();

        //1,  PUT method, should return NOT_FOUND
        HttpServiceRequest request1 = new HttpServiceRequest(null, HttpServer.Method.PUT, null);
        HttpServiceResponse response1 = listBookieInfoService.handle(request1);
        assertEquals(HttpServer.StatusCode.NOT_FOUND.getValue(), response1.getStatusCode());

        //2, GET method, expected get 3 bookies info and the cluster total info
        HttpServiceRequest request2 = new HttpServiceRequest(null, HttpServer.Method.GET, null);
        HttpServiceResponse response2 = listBookieInfoService.handle(request2);
        assertEquals(HttpServer.StatusCode.OK.getValue(), response2.getStatusCode());
        @SuppressWarnings("unchecked")
        HashMap<String, String> respBody = JsonUtil.fromJson(response2.getBody(), HashMap.class);
        assertEquals(4, respBody.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(true, respBody.containsKey(getBookie(i).toString()));
        }
    }

    @Test
    public void testListDiskFilesService() throws Exception {
        baseConf.setZkServers(zkUtil.getZooKeeperConnectString());
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

        HttpService listDiskFileService = bkHttpServiceProvider.provideListDiskFileService();

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
}
