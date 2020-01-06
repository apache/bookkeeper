/*
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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests of the main BookKeeper client and the BP-41 bookieAddressTracking feature.
 */
@Slf4j
public class BookieNetworkAddressChangeTest extends BookKeeperClusterTestCase {

    public BookieNetworkAddressChangeTest() {
        super(1);
        this.useUUIDasBookieId = true;
    }

    @Test
    public void testFollowBookieAddressChange() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try (BookKeeper bkc = BookKeeper.newBuilder(conf)
                        .build();) {
            long lId;
            try (WriteHandle h = bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withPassword(new byte[0])
                    .execute()
                    .get();) {
                lId = h.getId();
                h.append("foo".getBytes("utf-8"));
            }

            // restart bookie, change port
            // on metadata we have a bookieId, not the network address
            restartBookies(c -> c);

            try (ReadHandle h = bkc
                    .newOpenLedgerOp()
                    .withLedgerId(lId)
                    .withRecovery(true)
                    .withPassword(new byte[0])
                    .execute()
                    .get()) {
                assertEquals(0, h.getLastAddConfirmed());
                try (LedgerEntries entries = h.read(0, 0);) {
                    assertEquals("foo", new String(entries.getEntry(0).getEntryBytes(), "utf-8"));
                }
            }
        }
    }

    @Test
    @Ignore("PLSR-1850 Seems like restart of the bookie always comes up on same port hence failing this test")
    public void testFollowBookieAddressChangeTrckingDisabled() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setEnableBookieAddressTracking(false);
        try (BookKeeper bkc = BookKeeper.newBuilder(conf)
                        .build();) {
            long lId;
            try (WriteHandle h = bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withPassword(new byte[0])
                    .execute()
                    .get();) {
                lId = h.getId();
                h.append("foo".getBytes("utf-8"));
            }

            // restart bookie, change port
            // on metadata we have a bookieId, not the network address
            restartBookie(getBookie(0));
            try (ReadHandle h = bkc
                    .newOpenLedgerOp()
                    .withLedgerId(lId)
                    .withRecovery(true)
                    .withPassword(new byte[0])
                    .execute()
                    .get()) {
                try (LedgerEntries entries = h.read(0, 0);) {
                    fail("Should not be able to connect to the bookie with Bookie Address Tracking Disabled");
                } catch (BKBookieHandleNotAvailableException expected) {
                }
            }
        }
    }

    @Test
    public void testFollowBookieAddressChangeZkSessionExpire() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try (BookKeeper bkc = BookKeeper.newBuilder(conf)
                        .build();) {
            long lId;
            try (WriteHandle h = bkc
                    .newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withPassword(new byte[0])
                    .execute()
                    .get();) {
                lId = h.getId();
                h.append("foo".getBytes("utf-8"));
            }

            log.error("expiring ZK session!");
            // expire zk session
            ZKRegistrationClient regClient = (ZKRegistrationClient) ((org.apache.bookkeeper.client.BookKeeper) bkc)
                    .getMetadataClientDriver()
                    .getRegistrationClient();

            regClient.getZk().getTestable().injectSessionExpiration();

            // restart bookie, change port
            // on metadata we have a bookieId, not the network address
            restartBookies(c -> c);

            try (ReadHandle h = bkc
                    .newOpenLedgerOp()
                    .withLedgerId(lId)
                    .withRecovery(true)
                    .withPassword(new byte[0])
                    .execute()
                    .get()) {
                assertEquals(0, h.getLastAddConfirmed());
                try (LedgerEntries entries = h.read(0, 0);) {
                    assertEquals("foo", new String(entries.getEntry(0).getEntryBytes(), "utf-8"));
                }
            }
        }
    }
}