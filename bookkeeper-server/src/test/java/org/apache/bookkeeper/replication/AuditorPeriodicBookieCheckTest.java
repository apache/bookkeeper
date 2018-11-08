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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdapter;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client.
 */
public class AuditorPeriodicBookieCheckTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicBookieCheckTest.class);

    private AuditorElector auditorElector = null;

    private static final int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicBookieCheckTest() {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setAuditorPeriodicBookieCheckInterval(CHECK_INTERVAL);
        conf.setMetadataServiceUri(metadataServiceUri);
        String addr = bs.get(0).getLocalAddress().toString();

        auditorElector = new AuditorElector(addr, conf);
        auditorElector.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        auditorElector.shutdown();

        super.tearDown();
    }

    /**
     * Test that the periodic bookie checker works.
     */
    @Test
    public void testPeriodicBookieCheckInterval() throws Exception {
        bsConfs.get(0).setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        runFunctionWithLedgerManagerFactory(bsConfs.get(0), mFactory -> {
            try (LedgerManager ledgerManager = mFactory.newLedgerManager()) {
                @Cleanup final LedgerUnderreplicationManager underReplicationManager =
                    mFactory.newLedgerUnderreplicationManager();

                LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
                LedgerMetadata md = LedgerHandleAdapter.getLedgerMetadata(lh);
                List<BookieSocketAddress> ensemble = new ArrayList<>(md.getAllEnsembles().get(0L));
                ensemble.set(0, new BookieSocketAddress("1.1.1.1", 1000));
                md.updateEnsemble(0L, ensemble);

                GenericCallbackFuture<LedgerMetadata> cb =
                    new GenericCallbackFuture<LedgerMetadata>();
                ledgerManager.writeLedgerMetadata(lh.getId(), md, cb);
                cb.get();

                long underReplicatedLedger = -1;
                for (int i = 0; i < 10; i++) {
                    underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
                    if (underReplicatedLedger != -1) {
                        break;
                    }
                    Thread.sleep(CHECK_INTERVAL * 1000);
                }
                assertEquals("Ledger should be under replicated", lh.getId(), underReplicatedLedger);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
            return null;
        });
    }
}
