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
package org.apache.distributedlog.bk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.time.Duration;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.BookKeeperClientBuilder;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.bk.SimpleLedgerAllocator.AllocationException;
import org.apache.distributedlog.bk.SimpleLedgerAllocator.Phase;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.util.Transaction.OpListener;
import org.apache.distributedlog.util.Utils;
import org.apache.distributedlog.zk.DefaultZKOp;
import org.apache.distributedlog.zk.ZKTransaction;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestLedgerAllocator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestLedgerAllocator extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestLedgerAllocator.class);

    private static final String ledgersPath = "/ledgers";
    private static final OpListener<LedgerHandle> NULL_LISTENER = new OpListener<LedgerHandle>() {
        @Override
        public void onCommit(LedgerHandle r) {
            // no-op
        }

        @Override
        public void onAbort(Throwable t) {
            // no-op
        }
    };

    private ZooKeeperClient zkc;
    private BookKeeperClient bkc;
    private DistributedLogConfiguration dlConf = new DistributedLogConfiguration();

    private URI createURI(String path) {
        return URI.create("distributedlog://" + zkServers + path);
    }

    @BeforeAll
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .uri(createURI("/"))
                .zkServers(zkServers)
                .build();
        bkc = BookKeeperClientBuilder.newBuilder().name("bkc")
                .dlConfig(dlConf).ledgersPath(ledgersPath).zkc(zkc).build();
    }

    @AfterAll
    public void teardown() throws Exception {
        bkc.close();
        zkc.close();
    }

    private QuorumConfigProvider newQuorumConfigProvider(DistributedLogConfiguration conf) {
        return new ImmutableQuorumConfigProvider(conf.getQuorumConfig());
    }

    private ZKTransaction newTxn() {
        return new ZKTransaction(zkc);
    }

    private SimpleLedgerAllocator createAllocator(String allocationPath) throws Exception {
        return createAllocator(allocationPath, dlConf);
    }

    private SimpleLedgerAllocator createAllocator(String allocationPath,
                                                  DistributedLogConfiguration conf) throws Exception {
        return createAllocator(allocationPath, conf, null);
    }

    private SimpleLedgerAllocator createAllocator(String allocationPath,
                                                  DistributedLogConfiguration conf,
                                                  LedgerMetadata ledgerMetadata) throws Exception {
        return Utils.ioResult(SimpleLedgerAllocator.of(allocationPath, null,
                newQuorumConfigProvider(conf), zkc, bkc, ledgerMetadata));
    }

    @FlakyTest("https://issues.apache.org/jira/browse/DL-43")
    public void testAllocation() throws Exception {
        String allocationPath = "/allocation1";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
        logger.info("Try obtaining ledger handle {}", lh.getId());
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
        txn.addOp(DefaultZKOp.of(Op.setData("/unexistedpath", "data".getBytes(UTF_8), -1), null));
        try {
            Utils.ioResult(txn.execute());
            fail("Should fail the transaction when setting unexisted path");
        } catch (ZKException ke) {
            // expected
            logger.info("Should fail on executing transaction when setting unexisted path", ke);
        }
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));

        // Create new transaction to obtain the ledger again.
        txn = newTxn();
        // we could obtain the ledger if it was obtained
        LedgerHandle newLh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
        assertEquals(lh.getId(), newLh.getId());
        Utils.ioResult(txn.execute());
        data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        Utils.close(allocator);
    }

    @Test
    public void testBadVersionOnTwoAllocators() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/allocation-bad-version";
            zkc.get().create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(allocationPath, false, stat);
            Versioned<byte[]> allocationData = new Versioned<byte[]>(data, new LongVersion(stat.getVersion()));

            SimpleLedgerAllocator allocator1 =
                    new SimpleLedgerAllocator(allocationPath, allocationData, newQuorumConfigProvider(dlConf),
                            zkc, bkc);
            SimpleLedgerAllocator allocator2 =
                    new SimpleLedgerAllocator(allocationPath, allocationData, newQuorumConfigProvider(dlConf),
                            zkc, bkc);
            allocator1.allocate();
            // wait until allocated
            ZKTransaction txn1 = newTxn();
            LedgerHandle lh = Utils.ioResult(allocator1.tryObtain(txn1, NULL_LISTENER));
            allocator2.allocate();
            ZKTransaction txn2 = newTxn();
            try {
                Utils.ioResult(allocator2.tryObtain(txn2, NULL_LISTENER));
                fail("Should fail allocating on second allocator as allocator1 is starting allocating something.");
            } catch (ZKException ke) {
                assertEquals(KeeperException.Code.BADVERSION, ke.getKeeperExceptionCode());
            }
            Utils.ioResult(txn1.execute());
            Utils.close(allocator1);
            Utils.close(allocator2);

            long eid = lh.addEntry("hello world".getBytes());
            lh.close();
            LedgerHandle readLh = bkc.get().openLedger(lh.getId(),
                    BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
            Enumeration<LedgerEntry> entries = readLh.readEntries(eid, eid);
            int i = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("hello world", new String(entry.getEntry(), UTF_8));
                ++i;
            }
            assertEquals(1, i);
        });
    }

    @Test
    public void testAllocatorWithoutEnoughBookies() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/allocator-without-enough-bookies";

            DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
            confLocal.addConfiguration(conf);
            confLocal.setEnsembleSize(numBookies * 2);
            confLocal.setWriteQuorumSize(numBookies * 2);

            SimpleLedgerAllocator allocator1 = createAllocator(allocationPath, confLocal);
            allocator1.allocate();
            ZKTransaction txn1 = newTxn();

            try {
                Utils.ioResult(allocator1.tryObtain(txn1, NULL_LISTENER));
                fail("Should fail allocating ledger if there aren't enough bookies");
            } catch (AllocationException ioe) {
                // expected
                assertEquals(Phase.ERROR, ioe.getPhase());
            }
            byte[] data = zkc.get().getData(allocationPath, false, null);
            assertEquals(0, data.length);
        });
    }

    @Test
    public void testSuccessAllocatorShouldDeleteUnusedLedger() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/allocation-delete-unused-ledger";
            zkc.get().create(allocationPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat stat = new Stat();
            byte[] data = zkc.get().getData(allocationPath, false, stat);

            Versioned<byte[]> allocationData = new Versioned<byte[]>(data, new LongVersion(stat.getVersion()));

            SimpleLedgerAllocator allocator1 = new SimpleLedgerAllocator(allocationPath, allocationData,
                    newQuorumConfigProvider(dlConf), zkc, bkc);
            allocator1.allocate();
            // wait until allocated
            ZKTransaction txn1 = newTxn();
            LedgerHandle lh1 = Utils.ioResult(allocator1.tryObtain(txn1, NULL_LISTENER));

            // Second allocator kicks in
            stat = new Stat();
            data = zkc.get().getData(allocationPath, false, stat);
            allocationData = new Versioned<byte[]>(data, new LongVersion(stat.getVersion()));
            SimpleLedgerAllocator allocator2 = new SimpleLedgerAllocator(allocationPath, allocationData,
                    newQuorumConfigProvider(dlConf), zkc, bkc);
            allocator2.allocate();
            // wait until allocated
            ZKTransaction txn2 = newTxn();
            LedgerHandle lh2 = Utils.ioResult(allocator2.tryObtain(txn2, NULL_LISTENER));

            // should fail to commit txn1 as version is changed by second allocator
            try {
                Utils.ioResult(txn1.execute());
                fail("Should fail commit obtaining ledger handle from first allocator"
                        + " as allocator is modified by second allocator.");
            } catch (ZKException ke) {
                // as expected
            }
            Utils.ioResult(txn2.execute());
            Utils.close(allocator1);
            Utils.close(allocator2);

            // ledger handle should be deleted
            try {
                lh1.close();
                fail("LedgerHandle allocated by allocator1 should be deleted.");
            } catch (BKException bke) {
                // as expected
            }
            try {
                bkc.get().openLedger(lh1.getId(), BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
                fail("LedgerHandle allocated by allocator1 should be deleted.");
            } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException nslee) {
                // as expected
            }
            long eid = lh2.addEntry("hello world".getBytes());
            lh2.close();
            LedgerHandle readLh = bkc.get().openLedger(lh2.getId(),
                    BookKeeper.DigestType.CRC32, dlConf.getBKDigestPW().getBytes());
            Enumeration<LedgerEntry> entries = readLh.readEntries(eid, eid);
            int i = 0;
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("hello world", new String(entry.getEntry(), UTF_8));
                ++i;
            }
            assertEquals(1, i);
        });
    }

    @Test
    public void testCloseAllocatorDuringObtaining() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/allocation2";
            SimpleLedgerAllocator allocator = createAllocator(allocationPath);
            allocator.allocate();
            ZKTransaction txn = newTxn();
            // close during obtaining ledger.
            LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
            Utils.close(allocator);
            byte[] data = zkc.get().getData(allocationPath, false, null);
            assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
            // the ledger is not deleted
            bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                    dlConf.getBKDigestPW().getBytes(UTF_8));
        });
    }

    @FlakyTest("https://issues.apache.org/jira/browse/DL-26")
    public void testCloseAllocatorAfterConfirm() throws Exception {
        String allocationPath = "/allocation2";
        SimpleLedgerAllocator allocator = createAllocator(allocationPath);
        allocator.allocate();
        ZKTransaction txn = newTxn();
        // close during obtaining ledger.
        LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
        Utils.ioResult(txn.execute());
        Utils.close(allocator);
        byte[] data = zkc.get().getData(allocationPath, false, null);
        assertEquals(0, data.length);
        // the ledger is not deleted.
        bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                dlConf.getBKDigestPW().getBytes(UTF_8));
    }

    @Test
    public void testCloseAllocatorAfterAbort() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/allocation3";
            SimpleLedgerAllocator allocator = createAllocator(allocationPath);
            allocator.allocate();
            ZKTransaction txn = newTxn();
            // close during obtaining ledger.
            LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
            txn.addOp(DefaultZKOp.of(Op.setData("/unexistedpath", "data".getBytes(UTF_8), -1), null));
            try {
                Utils.ioResult(txn.execute());
                fail("Should fail the transaction when setting unexisted path");
            } catch (ZKException ke) {
                // expected
            }
            Utils.close(allocator);
            byte[] data = zkc.get().getData(allocationPath, false, null);
            assertEquals((Long) lh.getId(), Long.valueOf(new String(data, UTF_8)));
            // the ledger is not deleted.
            bkc.get().openLedger(lh.getId(), BookKeeper.DigestType.CRC32,
                    dlConf.getBKDigestPW().getBytes(UTF_8));
        });
    }

    @Test
    public void testConcurrentAllocation() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/" + testName;
            SimpleLedgerAllocator allocator = createAllocator(allocationPath);
            allocator.allocate();
            ZKTransaction txn1 = newTxn();
            CompletableFuture<LedgerHandle> obtainFuture1 = allocator.tryObtain(txn1, NULL_LISTENER);
            ZKTransaction txn2 = newTxn();
            CompletableFuture<LedgerHandle> obtainFuture2 = allocator.tryObtain(txn2, NULL_LISTENER);
            assertTrue(obtainFuture2.isDone());
            assertTrue(obtainFuture2.isCompletedExceptionally());
            try {
                Utils.ioResult(obtainFuture2);
                fail("Should fail the concurrent obtain since there is "
                        + "already a transaction obtaining the ledger handle");
            } catch (SimpleLedgerAllocator.ConcurrentObtainException cbe) {
                // expected
            }
        });
    }

    @Test
    public void testObtainMultipleLedgers() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/" + testName;
            SimpleLedgerAllocator allocator = createAllocator(allocationPath);
            int numLedgers = 10;
            Set<LedgerHandle> allocatedLedgers = new HashSet<LedgerHandle>();
            for (int i = 0; i < numLedgers; i++) {
                allocator.allocate();
                ZKTransaction txn = newTxn();
                LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
                Utils.ioResult(txn.execute());
                allocatedLedgers.add(lh);
            }
            assertEquals(numLedgers, allocatedLedgers.size());
        });
    }

    @Test
    public void testAllocationWithMetadata() throws Exception {
        assertTimeout(Duration.ofMinutes(1), () -> {
            String allocationPath = "/" + testName;

            String application = "testApplicationMetadata";
            String component = "testComponentMetadata";
            String custom = "customMetadata";
            LedgerMetadata ledgerMetadata = new LedgerMetadata();
            ledgerMetadata.setApplication(application);
            ledgerMetadata.setComponent(component);
            ledgerMetadata.addCustomMetadata("custom", custom);

            SimpleLedgerAllocator allocator = createAllocator(allocationPath, dlConf, ledgerMetadata);
            allocator.allocate();

            ZKTransaction txn = newTxn();
            LedgerHandle lh = Utils.ioResult(allocator.tryObtain(txn, NULL_LISTENER));
            Map<String, byte[]> customMeta = lh.getCustomMetadata();
            assertEquals(application, new String(customMeta.get("application"), UTF_8));
            assertEquals(component, new String(customMeta.get("component"), UTF_8));
            assertEquals(custom, new String(customMeta.get("custom"), UTF_8));
        });
    }
}
