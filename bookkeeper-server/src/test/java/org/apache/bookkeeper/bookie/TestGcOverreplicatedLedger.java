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

package org.apache.bookkeeper.bookie;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerTestCase;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooDefs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test GC-overreplicated ledger.
 */
@RunWith(Parameterized.class)
public class TestGcOverreplicatedLedger extends LedgerManagerTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManager = ledgerManagerFactory.newLedgerManager();
        activeLedgers = new SnapshotMap<Long, Boolean>();
    }

    public TestGcOverreplicatedLedger(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls, 3);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { HierarchicalLedgerManagerFactory.class } });
    }

    @Test
    public void testGcOverreplicatedLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        final AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<Versioned<LedgerMetadata>>() {

            @Override
            public void operationComplete(int rc, Versioned<LedgerMetadata> result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result.getValue());
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata.get());
        ServerConfiguration bkConf = getBkConf(bookieNotInEnsemble);
        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertFalse(activeLedgers.containsKey(lh.getId()));
    }

    @Test
    public void testNoGcOfLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        final AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<Versioned<LedgerMetadata>>() {

            @Override
            public void operationComplete(int rc, Versioned<LedgerMetadata> result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result.getValue());
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress address = null;
        SortedMap<Long, ? extends List<BookieSocketAddress>> ensembleMap = newLedgerMetadata.get().getAllEnsembles();
        for (List<BookieSocketAddress> ensemble : ensembleMap.values()) {
            address = ensemble.get(0);
        }
        ServerConfiguration bkConf = getBkConf(address);
        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    @Test
    public void testNoGcIfLedgerBeingReplicated() throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, DigestType.MAC, "".getBytes());
        activeLedgers.put(lh.getId(), true);

        final AtomicReference<LedgerMetadata> newLedgerMetadata = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(lh.getId(), new GenericCallback<Versioned<LedgerMetadata>>() {

            @Override
            public void operationComplete(int rc, Versioned<LedgerMetadata> result) {
                if (rc == BKException.Code.OK) {
                    newLedgerMetadata.set(result.getValue());
                }
                latch.countDown();
            }
        });
        latch.await();
        if (newLedgerMetadata.get() == null) {
            Assert.fail("No ledger metadata found");
        }
        BookieSocketAddress bookieNotInEnsemble = getBookieNotInEnsemble(newLedgerMetadata.get());
        ServerConfiguration bkConf = getBkConf(bookieNotInEnsemble);
        bkConf.setGcOverreplicatedLedgerWaitTime(10, TimeUnit.MILLISECONDS);

        lh.close();

        ZkLedgerUnderreplicationManager.acquireUnderreplicatedLedgerLock(
            zkc,
            ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf),
            lh.getId(),
            ZooDefs.Ids.OPEN_ACL_UNSAFE);

        final CompactableLedgerStorage mockLedgerStorage = new MockLedgerStorage();
        final GarbageCollector garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, mockLedgerStorage,
                bkConf, NullStatsLogger.INSTANCE);
        Thread.sleep(bkConf.getGcOverreplicatedLedgerWaitTimeMillis() + 1);
        garbageCollector.gc(new GarbageCleaner() {

            @Override
            public void clean(long ledgerId) {
                try {
                    mockLedgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }
        });

        Assert.assertTrue(activeLedgers.containsKey(lh.getId()));
    }

    private BookieSocketAddress getBookieNotInEnsemble(LedgerMetadata ledgerMetadata) throws UnknownHostException {
        List<BookieSocketAddress> allAddresses = Lists.newArrayList();
        for (BookieServer bk : bs) {
            allAddresses.add(bk.getLocalAddress());
        }
        SortedMap<Long, ? extends List<BookieSocketAddress>> ensembles = ledgerMetadata.getAllEnsembles();
        for (List<BookieSocketAddress> fragmentEnsembles : ensembles.values()) {
            allAddresses.removeAll(fragmentEnsembles);
        }
        Assert.assertEquals(allAddresses.size(), 1);
        return allAddresses.get(0);
    }
}
