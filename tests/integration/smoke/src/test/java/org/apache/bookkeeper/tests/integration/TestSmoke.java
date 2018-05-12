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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.tests.BookKeeperClusterUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@Slf4j
@RunWith(Arquillian.class)
public class TestSmoke {
    private static byte[] PASSWD = "foobar".getBytes();

    @ArquillianResource
    DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Before
    public void setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));
    }

    @After
    public void teardown() throws Exception {
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Test
    public void testReadWrite() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        try (BookKeeper bk = new BookKeeper(zookeeper)) {
            long ledgerId;
            try (LedgerHandle writelh = bk.createLedger(BookKeeper.DigestType.CRC32C, PASSWD)) {
                ledgerId = writelh.getId();
                for (int i = 0; i < 100; i++) {
                    writelh.addEntry(("entry-" + i).getBytes());
                }
            }

            try (LedgerHandle readlh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWD)) {
                long lac = readlh.getLastAddConfirmed();
                int i = 0;
                Enumeration<LedgerEntry> entries = readlh.readEntries(0, lac);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    String readBack = new String(e.getEntry());
                    assertEquals(readBack, "entry-" + i++);
                }
                assertEquals(i, 100);
            }
        }
    }

    @Test
    public void testReadWriteAdv() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        try (BookKeeper bk = new BookKeeper(zookeeper)) {
            long ledgerId;
            try (LedgerHandle writelh = bk.createLedgerAdv(3, 3, 2, BookKeeper.DigestType.CRC32C, PASSWD)) {
                ledgerId = writelh.getId();
                for (int i = 0; i < 100; i++) {
                    writelh.addEntry(i, ("entry-" + i).getBytes());
                }
            }

            try (LedgerHandle readlh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32C, PASSWD)) {
                long lac = readlh.getLastAddConfirmed();
                int i = 0;
                Enumeration<LedgerEntry> entries = readlh.readEntries(0, lac);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    String readBack = new String(e.getEntry());
                    assertEquals(readBack, "entry-" + i++);
                }
                assertEquals(i, 100);
            }
        }
    }

    @Test
    public void testTailingReads() throws Exception {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        @Cleanup BookKeeper bk = new BookKeeper(zookeeper);
        @Cleanup LedgerHandle writeLh = bk.createLedger(DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ExecutorService writeExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("write-executor").build());

        @Cleanup LedgerHandle readLh = bk.openLedgerNoRecovery(writeLh.getId(), DigestType.CRC32C, PASSWD);
        @Cleanup("shutdown") ExecutorService readExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("read-executor").build());

        int numEntries = 100;
        CompletableFuture<Void> readFuture = new CompletableFuture<>();
        CompletableFuture<Void> writeFuture = new CompletableFuture<>();

        // start the read thread
        readExecutor.submit(() -> {
            long lastExpectedConfirmedEntryId = numEntries - 2;
            long nextEntryId = 0L;
            try {
                while (nextEntryId <= lastExpectedConfirmedEntryId) {
                    long lac = readLh.getLastAddConfirmed();
                    while (lac >= nextEntryId) {
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
                    while (readLh.readLastConfirmed() < nextEntryId) {
                        TimeUnit.MILLISECONDS.sleep(100L);
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

        // both write and read should be successful
        result(readFuture);
        result(writeFuture);

        assertEquals(numEntries - 2, readLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddConfirmed());
        assertEquals(numEntries - 1, writeLh.getLastAddPushed());
    }

}
