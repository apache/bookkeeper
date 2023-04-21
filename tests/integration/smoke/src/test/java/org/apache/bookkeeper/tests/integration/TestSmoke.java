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
package org.apache.bookkeeper.tests.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import com.github.dockerjava.api.DockerClient;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Smoke tests for ledger apis.
 */
@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSmoke {
    static final byte[] PASSWD = "foobar".getBytes();

    @ArquillianResource
    DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Test
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));
    }

    @Test
    public void tear999_Teardown() {
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Test
    public void test001_ReadWrite() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        int numEntries = 100;
        try (BookKeeper bk = new BookKeeper(zookeeper)) {
            long ledgerId;
            try (LedgerHandle writelh = bk.createLedger(BookKeeper.DigestType.CRC32C, PASSWD)) {
                ledgerId = writelh.getId();
                for (int i = 0; i < numEntries; i++) {
                    writelh.addEntry(("entry-" + i).getBytes());
                }
            }

            readEntries(bk, ledgerId, numEntries);
        }
    }

    @Test
    public void test002_ReadWriteAdv() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        int numEntries = 100;
        try (BookKeeper bk = new BookKeeper(zookeeper)) {
            long ledgerId;
            try (LedgerHandle writelh = bk.createLedgerAdv(3, 3, 2, BookKeeper.DigestType.CRC32C, PASSWD)) {
                ledgerId = writelh.getId();
                for (int i = 0; i < numEntries; i++) {
                    writelh.addEntry(i, ("entry-" + i).getBytes());
                }
            }

            readEntries(bk, ledgerId, numEntries);
        }
    }

    static void readEntries(BookKeeper bk,
                            long ledgerId,
                            int numExpectedEntries) throws Exception {
        try (LedgerHandle readlh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWD)) {
            long lac = readlh.getLastAddConfirmed();
            int i = 0;
            Enumeration<LedgerEntry> entries = readlh.readEntries(0, lac);
            while (entries.hasMoreElements()) {
                LedgerEntry e = entries.nextElement();
                String readBack = new String(e.getEntry());
                assertEquals(readBack, "entry-" + i++);
            }
            assertEquals(i, numExpectedEntries);
        }
    }

    @Test
    public void test003_TailingReadsWithoutExplicitLac() throws Exception {
        testTailingReads(100, 98, 0);
    }

    @Test
    public void test004_TailingReadsWithExplicitLac() throws Exception {
        testTailingReads(100, 99, 100);
    }

    private void testTailingReads(int numEntries,
                                  long lastExpectedConfirmedEntryId,
                                  int lacIntervalMs)
            throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        ClientConfiguration conf = new ClientConfiguration()
            .setExplictLacInterval(lacIntervalMs)
            .setMetadataServiceUri("zk://" + zookeeper + "/ledgers");
        @Cleanup BookKeeper bk = BookKeeper.forConfig(conf).build();
        @Cleanup LedgerHandle writeLh = bk.createLedger(DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ExecutorService writeExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("write-executor").build());

        @Cleanup LedgerHandle readLh = bk.openLedgerNoRecovery(writeLh.getId(), DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ExecutorService readExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("read-executor").build());

        CompletableFuture<Void> readFuture = new CompletableFuture<>();
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();

        // start the read thread
        readExecutor.submit(() -> {
            long nextEntryId = 0L;
            try {
                while (nextEntryId <= lastExpectedConfirmedEntryId) {
                    long lac = readLh.getLastAddConfirmed();
                    while (lac >= nextEntryId) {
                        log.info("Attempt to read entries : [{} - {}]",
                            nextEntryId, lac);
                        Enumeration<LedgerEntry> entries = readLh.readEntries(nextEntryId, lac);
                        while (entries.hasMoreElements()) {
                            LedgerEntry e = entries.nextElement();
                            String readBack = new String(e.getEntry(), UTF_8);
                            log.info("Read entry {} : {}", e.getEntryId(), readBack);
                            assertEquals(readBack, "entry-" + (nextEntryId++));
                        }
                        assertEquals(lac + 1, nextEntryId);
                    }

                    if (nextEntryId > lastExpectedConfirmedEntryId) {
                        break;
                    }

                    // refresh lac
                    readLh.readLastConfirmed();
                    while (readLh.getLastAddConfirmed() < nextEntryId) {
                        log.info("Refresh lac {}, next entry id = {}",
                            readLh.getLastAddConfirmed(), nextEntryId);
                        TimeUnit.MILLISECONDS.sleep(100L);

                        readLh.readLastConfirmed();
                        if (readLh.getLastAddConfirmed() < nextEntryId) {
                            readLh.readExplicitLastConfirmed();
                        }
                    }
                }
                FutureUtils.complete(readFuture, null);
                log.info("Completed tailing read ledger {}", writeLh.getId());
            } catch (Exception e) {
                log.error("Exceptions thrown during tailing read ledger {}", writeLh.getId(), e);
                readFuture.completeExceptionally(e);
            }
        });

        // start the write thread
        writeEntries(numEntries, writeLh, writeExecutor, writeFuture);

        // both write and read should be successful
        result(readFuture);
        result(writeFuture);

        assertEquals(lastExpectedConfirmedEntryId, readLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddPushed());
    }

    private static void writeEntries(int numEntries,
                                     LedgerHandle writeLh,
                                     ExecutorService writeExecutor,
                                     CompletableFuture<Void> writeFuture) {
        writeExecutor.submit(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    writeLh.addEntry(("entry-" + i).getBytes());
                }
                log.info("Completed writing {} entries to ledger {}", numEntries, writeLh.getId());
                FutureUtils.complete(writeFuture, null);
            } catch (Exception e) {
                log.error("Exceptions thrown during writing {} entries to ledger {}", numEntries, writeLh.getId(), e);
                writeFuture.completeExceptionally(e);
            }
        });
    }

    @Test
    public void test005_LongTailingReadsWithoutExplicitLac() throws Exception {
        testLongPollTailingReads(100, 98, 0);
    }

    @Test
    public void test006_LongTailingReadsWithExplicitLac() throws Exception {
        testLongPollTailingReads(100, 99, 100);
    }

    private void testLongPollTailingReads(int numEntries,
                                          long lastExpectedConfirmedEntryId,
                                          int lacIntervalMs)
            throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        ClientConfiguration conf = new ClientConfiguration()
            .setExplictLacInterval(lacIntervalMs)
            .setMetadataServiceUri("zk://" + zookeeper + "/ledgers");
        @Cleanup BookKeeper bk = BookKeeper.forConfig(conf).build();
        @Cleanup LedgerHandle writeLh = bk.createLedger(DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ExecutorService writeExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("write-executor").build());

        @Cleanup LedgerHandle readLh = bk.openLedgerNoRecovery(writeLh.getId(), DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ScheduledExecutorService readExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("read-executor").build());

        CompletableFuture<Void> readFuture = new CompletableFuture<>();
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();

        // start the read thread
        AtomicLong nextEntryId = new AtomicLong(0L);

        Runnable readNextFunc = new Runnable() {

            @Override
            public void run() {
                if (nextEntryId.get() > lastExpectedConfirmedEntryId) {
                    FutureUtils.complete(readFuture, null);
                    return;
                }

                Stopwatch readWatch = Stopwatch.createStarted();
                log.info("Attempt to read next entry {} - lac {}", nextEntryId.get(), readLh.getLastAddConfirmed());
                readLh.asyncReadLastConfirmedAndEntry(nextEntryId.get(), Long.MAX_VALUE / 2, false,
                    (rc, lastConfirmed, entry, ctx) -> {
                        log.info("Read return in {} ms : rc = {}, lac = {}, entry = {}",
                            readWatch.elapsed(TimeUnit.MILLISECONDS), rc, lastConfirmed, entry);
                        if (Code.OK == rc) {
                            if (null != entry) {
                                log.info("Successfully read entry {} : {}",
                                    entry.getEntryId(), new String(entry.getEntry(), UTF_8));
                                if (entry.getEntryId() != nextEntryId.get()) {
                                    log.error("Attempt to read entry {} but received entry {}",
                                        nextEntryId.get(), entry.getEntryId());
                                    readFuture.completeExceptionally(
                                        BKException.create(Code.UnexpectedConditionException));
                                    return;
                                } else {
                                    nextEntryId.incrementAndGet();
                                }
                            }
                            readExecutor.submit(this);
                        } else if (Code.NoSuchLedgerExistsException == rc) {
                            // the ledger hasn't been created yet.
                            readExecutor.schedule(this, 200, TimeUnit.MILLISECONDS);
                        } else {
                            log.error("Failed to read entries : {}", BKException.getMessage(rc));
                            readFuture.completeExceptionally(BKException.create(rc));
                        }
                    }, null);
            }
        };

        readNextFunc.run();

        // start the write thread
        writeEntries(numEntries, writeLh, writeExecutor, writeFuture);

        // both write and read should be successful
        result(readFuture);
        result(writeFuture);

        assertEquals(lastExpectedConfirmedEntryId + 1, nextEntryId.get());
        assertEquals(lastExpectedConfirmedEntryId, readLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddPushed());
    }

}
