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
package org.apache.bookkeeper.server.http.service;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link ListLedgerService}.
 */
public class ListLedgerServiceTest extends BookKeeperClusterTestCase {
    private final ObjectMapper mapper = new ObjectMapper();
    private ListLedgerService listLedgerService;

    public ListLedgerServiceTest() {
        super(1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        StatsProvider provider = new TestStatsProvider();
        listLedgerService = new ListLedgerService(confByIndex(0),
                BookieResources.createMetadataDriver(confByIndex(0),
                        provider.getStatsLogger("")).getLedgerManagerFactory());
    }

    @Test
    public void testEmptyList() throws Exception {
        HttpServiceResponse response = listLedgerService.handle(new HttpServiceRequest());
        assertEquals(response.getStatusCode(), HttpServer.StatusCode.OK.getValue());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(0, json.size());
    }

    @Test
    public void testListLedgers() throws Exception {
        int ledgerNum = RandomUtils.nextInt(1, 10);
        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        for (int i = 0; i < ledgerNum; i++) {
            LedgerHandle ledger = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0]);
            ledgers.put(ledger.getId(), ledger.getLedgerMetadata());
            ledger.close();
        }

        HttpServiceResponse response = listLedgerService.handle(new HttpServiceRequest());
        assertEquals(response.getStatusCode(), HttpServer.StatusCode.OK.getValue());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(ledgerNum, json.size());

        json.fieldNames().forEachRemaining(field -> {
            assertTrue(ledgers.containsKey(Long.parseLong(field)));
            assertTrue(json.get(field).isNull());
        });
    }

    @Test
    public void testListLedgersWithMetadata() throws Exception {
        int ledgerNum = RandomUtils.nextInt(1, 10);
        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        for (int i = 0; i < ledgerNum; i++) {
            LedgerHandle ledger = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0]);
            ledger.close();
            ledgers.put(ledger.getId(), ledger.getLedgerMetadata());
        }

        HttpServiceResponse response = listLedgerService.handle(new HttpServiceRequest(null, HttpServer.Method.GET,
                ImmutableMap.of("print_metadata", "true")));
        assertEquals(response.getStatusCode(), HttpServer.StatusCode.OK.getValue());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(ledgerNum, json.size());

        json.fieldNames().forEachRemaining(field -> {
            LedgerMetadata meta = ledgers.get(Long.parseLong(field));
            assertNotNull(meta);
            assertFalse(json.get(field).isNull());
        });
    }

    @Test
    public void testListLedgersWithMetadataDecoded() throws Exception {
        int ledgerNum = RandomUtils.nextInt(1, 10);
        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        for (int i = 0; i < ledgerNum; i++) {
            LedgerHandle ledger = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0],
                    ImmutableMap.of("test_key", "test_value".getBytes()));
            ledger.close();
            ledgers.put(ledger.getId(), ledger.getLedgerMetadata());
        }

        HttpServiceResponse response = listLedgerService.handle(new HttpServiceRequest(null, HttpServer.Method.GET,
                ImmutableMap.of("print_metadata", "true", "decode_meta", "true")));
        assertEquals(response.getStatusCode(), HttpServer.StatusCode.OK.getValue());
        JsonNode json = mapper.readTree(response.getBody());
        assertEquals(ledgerNum, json.size());

        json.fieldNames().forEachRemaining(field -> {
            LedgerMetadata meta = ledgers.get(Long.parseLong(field));
            assertNotNull(meta);
            JsonNode node = json.get(field);
            assertEquals(meta.getMetadataFormatVersion(), node.get("metadataFormatVersion").asInt());
            assertEquals(meta.getEnsembleSize(), node.get("ensembleSize").asInt());
            assertEquals(meta.getWriteQuorumSize(), node.get("writeQuorumSize").asInt());
            assertEquals(meta.getAckQuorumSize(), node.get("ackQuorumSize").asInt());
            assertEquals(meta.getCToken(), node.get("ctoken").asLong());
//            assertEquals(meta.getCtime(), node.get("ctime").asLong());
            assertEquals(meta.getState().name(), node.get("state").asText());
            assertEquals(meta.isClosed(), node.get("closed").asBoolean());
            assertEquals(meta.getLength(), node.get("length").asLong());
            assertEquals(meta.getLastEntryId(), node.get("lastEntryId").asLong());
            assertEquals(meta.getDigestType().name(), node.get("digestType").asText());
            assertEquals(new String(meta.getPassword()), node.get("password").asText());

            for (Map.Entry<String, byte[]> entry : meta.getCustomMetadata().entrySet()) {
                JsonNode data = node.get("customMetadata").get(entry.getKey());
                assertArrayEquals(entry.getValue(), Base64.getDecoder().decode(data.asText()));
            }

            for (Map.Entry<Long, ? extends List<BookieId>> entry : meta.getAllEnsembles().entrySet()) {
                JsonNode members = node.get("allEnsembles")
                        .get(String.valueOf(entry.getKey()));
                assertEquals(1, entry.getValue().size());
                assertEquals(entry.getValue().size(), members.size());
                JsonNode member = members.get(0);
                assertEquals(entry.getValue().get(0).getId(), member.get("id").asText());
            }
        });
    }
}
