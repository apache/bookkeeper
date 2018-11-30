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

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test an EnsembleChange watcher.
 */
@RunWith(Parameterized.class)
public class TestWatchEnsembleChange extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestWatchEnsembleChange.class);

    final DigestType digestType;
    final Class<? extends LedgerManagerFactory> lmFactoryCls;

    public TestWatchEnsembleChange(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(7);
        this.digestType = DigestType.CRC32;
        this.lmFactoryCls = lmFactoryCls;
        baseClientConf.setLedgerManagerFactoryClass(lmFactoryCls);
        baseConf.setLedgerManagerFactoryClass(lmFactoryCls);
    }

    @SuppressWarnings("deprecation")
    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { org.apache.bookkeeper.meta.FlatLedgerManagerFactory.class },
                { HierarchicalLedgerManagerFactory.class },
                { LongHierarchicalLedgerManagerFactory.class },
                { org.apache.bookkeeper.meta.MSLedgerManagerFactory.class },
        });
    }

    @Test
    public void testWatchEnsembleChange() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
            LOG.info("Added entry {}.", i);
        }
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        long lastLAC = readLh.getLastAddConfirmed();
        assertEquals(numEntries - 2, lastLAC);
        List<BookieSocketAddress> ensemble =
            lh.getCurrentEnsemble();
        for (BookieSocketAddress addr : ensemble) {
            killBookie(addr);
        }
        // write another batch of entries, which will trigger ensemble change
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + (numEntries + i)).getBytes());
            LOG.info("Added entry {}.", (numEntries + i));
        }
        TimeUnit.SECONDS.sleep(5);
        readLh.readLastConfirmed();
        assertEquals(2 * numEntries - 2, readLh.getLastAddConfirmed());
        readLh.close();
        lh.close();
    }

    @Test
    public void testWatchMetadataRemoval() throws Exception {
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        runFunctionWithLedgerManagerFactory(baseConf, factory -> {
            try {
                testWatchMetadataRemoval(factory);
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
            return null;
        });
    }

    private void testWatchMetadataRemoval(LedgerManagerFactory factory) throws Exception {
        @Cleanup final LedgerManager manager = factory.newLedgerManager();
        @Cleanup LedgerIdGenerator idGenerator = factory.newLedgerIdGenerator();

        final ByteBuffer bbLedgerId = ByteBuffer.allocate(8);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);

        List<BookieSocketAddress> ensemble = Lists.newArrayList(
                new BookieSocketAddress("192.0.2.1", 1234),
                new BookieSocketAddress("192.0.2.2", 1234),
                new BookieSocketAddress("192.0.2.3", 1234),
                new BookieSocketAddress("192.0.2.4", 1234));
        idGenerator.generateLedgerId(new GenericCallback<Long>() {
                @Override
                public void operationComplete(int rc, final Long lid) {
                    LedgerMetadata metadata = LedgerMetadataBuilder.create()
                        .withEnsembleSize(4).withWriteQuorumSize(2)
                        .withAckQuorumSize(2)
                        .newEnsembleEntry(0L, ensemble).build();
                    manager.createLedgerMetadata(lid, metadata)
                        .whenComplete((result, exception) -> {
                                bbLedgerId.putLong(lid);
                                bbLedgerId.flip();
                                createLatch.countDown();
                            });
                }
            });

        assertTrue(createLatch.await(2000, TimeUnit.MILLISECONDS));
        final long createdLid = bbLedgerId.getLong();

        manager.registerLedgerMetadataListener(createdLid,
                new LedgerMetadataListener() {

            @Override
            public void onChanged(long ledgerId, Versioned<LedgerMetadata> metadata) {
                assertEquals(ledgerId, createdLid);
                assertEquals(metadata, null);
                removeLatch.countDown();
            }
        });

        manager.removeLedgerMetadata(createdLid, Version.ANY).get(2, TimeUnit.SECONDS);
        assertTrue(removeLatch.await(2, TimeUnit.SECONDS));
    }
}
