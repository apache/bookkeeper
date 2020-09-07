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

package org.apache.bookkeeper.test;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests writing to concurrent ledgers.
 */
public class ConcurrentLedgerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentLedgerTest.class);

    Bookie bookie;
    File txnDir, ledgerDir;
    int recvTimeout = 10000;
    Semaphore throttle;
    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
    final List<File> tempDirs = new ArrayList<File>();

    private File createTempDir(String prefix, String suffix, File parent) throws IOException {
        File dir = File.createTempFile(prefix, suffix, parent);
        dir.delete();
        tempDirs.add(dir);
        return dir;
    }

    @Before
    public void setUp() throws Exception {
        String txnDirName = System.getProperty("txnDir");
        if (txnDirName != null) {
            txnDir = new File(txnDirName);
        }
        String ledgerDirName = System.getProperty("ledgerDir");
        if (ledgerDirName != null) {
            ledgerDir = new File(ledgerDirName);
        }
        File tmpFile = createTempDir("book", ".txn", txnDir);
        txnDir = new File(tmpFile.getParent(), tmpFile.getName() + ".dir");
        txnDir.mkdirs();
        tmpFile = createTempDir("book", ".ledger", ledgerDir);
        ledgerDir = new File(tmpFile.getParent(), tmpFile.getName() + ".dir");
        ledgerDir.mkdirs();

        conf.setBookiePort(5000);
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });
        bookie = new Bookie(conf);
        bookie.start();
    }

    static void recursiveDelete(File f) {
        if (f.isFile()) {
            f.delete();
        } else {
            for (File i: f.listFiles()) {
                recursiveDelete(i);
            }
            f.delete();
        }
    }

    @After
    public void tearDown() {
        bookie.shutdown();
        recursiveDelete(txnDir);
        recursiveDelete(ledgerDir);
    }

    byte[] zeros = new byte[16];

    int iterations = 51;
    {
        String iterationsString = System.getProperty("iterations");
        if (iterationsString != null) {
            iterations = Integer.parseInt(iterationsString);
        }
    }
    int iterationStep = 25;
    {
        String iterationsString = System.getProperty("iterationStep");
        if (iterationsString != null) {
            iterationStep = Integer.parseInt(iterationsString);
        }
    }
    @Test
    public void testConcurrentWrite() throws IOException, InterruptedException, BookieException {
        int size = 1024;
        int totalwrites = 128;
        if (System.getProperty("totalwrites") != null) {
            totalwrites = Integer.parseInt(System.getProperty("totalwrites"));
        }
        LOG.info("Running up to " + iterations + " iterations");
        LOG.info("Total writes = " + totalwrites);
        int ledgers;
        for (ledgers = 1; ledgers <= iterations; ledgers += iterationStep) {
            long duration = doWrites(ledgers, size, totalwrites);
            LOG.info(totalwrites + " on " + ledgers + " took " + duration + " ms");
        }
        LOG.info("ledgers " + ledgers);
        for (ledgers = 1; ledgers <= iterations; ledgers += iterationStep) {
            long duration = doReads(ledgers, size, totalwrites);
            LOG.info(ledgers + " read " + duration + " ms");
        }
    }

    private long doReads(int ledgers, int size, int totalwrites)
            throws IOException, InterruptedException, BookieException {
        long start = System.currentTimeMillis();
        for (int i = 1; i <= totalwrites / ledgers; i++) {
            for (int j = 1; j <= ledgers; j++) {
                ByteBuf entry = bookie.readEntry(j, i);
                // skip the ledger id and the entry id
                entry.readLong();
                entry.readLong();
                assertEquals(j + "@" + i, j + 2, entry.readLong());
                assertEquals(j + "@" + i, i + 3, entry.readLong());
            }
        }
        long finish = System.currentTimeMillis();
        return finish - start;
    }
    private long doWrites(int ledgers, int size, int totalwrites)
            throws IOException, InterruptedException, BookieException {
        throttle = new Semaphore(10000);
        WriteCallback cb = new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                    BookieId addr, Object ctx) {
                AtomicInteger counter = (AtomicInteger) ctx;
                counter.getAndIncrement();
                throttle.release();
            }
        };
        AtomicInteger counter = new AtomicInteger();
        long start = System.currentTimeMillis();
        for (int i = 1; i <= totalwrites / ledgers; i++) {
            for (int j = 1; j <= ledgers; j++) {
                ByteBuffer bytes = ByteBuffer.allocate(size);
                bytes.putLong(j);
                bytes.putLong(i);
                bytes.putLong(j + 2);
                bytes.putLong(i + 3);
                bytes.put(("This is ledger " + j + " entry " + i).getBytes());
                bytes.position(0);
                bytes.limit(bytes.capacity());
                throttle.acquire();
                bookie.addEntry(Unpooled.wrappedBuffer(bytes), false, cb, counter, zeros);
            }
        }
        long finish = System.currentTimeMillis();
        return finish - start;
    }
}
