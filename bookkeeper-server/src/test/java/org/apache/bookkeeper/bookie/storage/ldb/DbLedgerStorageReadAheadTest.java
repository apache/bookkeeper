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

package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for read-ahead module.
 */
@Slf4j
public class DbLedgerStorageReadAheadTest {
    private DbLedgerStorage storage;
    private File tmpDir;

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new TestBookieImpl(conf);

        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    @Test
    public void testCanTriggerReadAheadTaskAfterHit() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        List<Pair<Pair<Long, Long>, Long>> locations = new ArrayList<>();
        final int ledgers = 2, lInit = 3;
        final int entries = 300, eInit = 4;

        for (int i = 0; i < ledgers; i++) {
            for (int j = 0; j < entries; j++) {
                ByteBuf entry = Unpooled.buffer(1024);
                long lid = lInit + i;
                long eid = eInit + j;
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeBytes(("entry-" + eid).getBytes());
                long location = entryLogger.addEntry(lid, entry, false);
                entryLocationIndex.addLocation(lid, eid, location);
                locations.add(Pair.of(Pair.of(lid, eid), location));
            }
        }

        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats());

        // first pos
        Pair<Pair<Long, Long>, Long> firstEntry = locations.get(0);
        long lid = firstEntry.getLeft().getLeft();
        long eid = firstEntry.getLeft().getRight();
        long location = firstEntry.getRight();

        readAheadManager.addNextReadPosition(lid, eid, lid, eid, location);
        assertTrue(readAheadManager.hitInReadAheadPositions(lid, eid));
    }

    @Test
    public void testSingleTaskBlockUntilReadAheadTaskComplete() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        List<Pair<Pair<Long, Long>, Long>> locations = new ArrayList<>();
        final int ledgers = 2, lInit = 3;
        final int entries = 300, eInit = 4;

        for (int i = 0; i < ledgers; i++) {
            for (int j = 0; j < entries; j++) {
                ByteBuf entry = Unpooled.buffer(1024);
                long lid = lInit + i;
                long eid = eInit + j;
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeBytes(("entry-" + eid).getBytes());
                long location = entryLogger.addEntry(lid, entry, false);
                entryLocationIndex.addLocation(lid, eid, location);
                locations.add(Pair.of(Pair.of(lid, eid), location));
            }
        }

        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats());

        // first pos
        Pair<Pair<Long, Long>, Long> firstEntry = locations.get(0);
        long lid = firstEntry.getLeft().getLeft();
        long eid = firstEntry.getLeft().getRight();
        long location = firstEntry.getRight();

        assertNull(cache.get(lid, eid));
        ByteBuf entry = readAheadManager.readEntryOrWait(lid, eid); // miss
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid, entry.getLong(8));

        assertNotNull(cache.get(lid, eid));
        entry = readAheadManager.readEntryOrWait(lid, eid + 1); // hit
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid + 1, entry.getLong(8));
    }

    @Test
    public void testMultiTaskBlockUntilReadAheadTaskComplete() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        List<Pair<Pair<Long, Long>, Long>> locations = new ArrayList<>();
        final int ledgers = 2, lInit = 3;
        final int entries = 300, eInit = 4;

        for (int i = 0; i < ledgers; i++) {
            for (int j = 0; j < entries; j++) {
                ByteBuf entry = Unpooled.buffer(1024);
                long lid = lInit + i;
                long eid = eInit + j;
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeBytes(("entry-" + eid).getBytes());
                long location = entryLogger.addEntry(lid, entry, false);
                entryLocationIndex.addLocation(lid, eid, location);
                locations.add(Pair.of(Pair.of(lid, eid), location));
            }
        }

        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats());

        // first pos
        Pair<Pair<Long, Long>, Long> firstEntry = locations.get(0);
        long lid = firstEntry.getLeft().getLeft();
        long eid = firstEntry.getLeft().getRight();
        long location = firstEntry.getRight();

        // multi reads
        readAheadManager.readEntryOrWait(lid, eid); // miss

        new Thread(() -> {
            try {
                assertNull(cache.get(lid, eid + 1));
                ByteBuf entry = readAheadManager.readEntryOrWait(lid, eid + 1);
                assertEquals(lid, entry.getLong(0));
                assertEquals(eid + 1, entry.getLong(8));
            } catch (Exception e) {

            }
        }).start();

        new Thread(() -> {
            // wait until the read-ahead task is actually executed
            while (cache.get(lid, eid + 1) != null) {
                // spin
            }

            try {
                ByteBuf entry = readAheadManager.readEntryOrWait(lid, eid + 95);
                assertEquals(lid, entry.getLong(0));
                assertEquals(eid + 95, entry.getLong(8));
            } catch (Exception e) {

            }
        }).start();
    }

    @Test
    public void testReadFewEntryAhead() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        List<Pair<Pair<Long, Long>, Long>> locations = new ArrayList<>();
        final int ledgers = 2, lInit = 3;
        final int entries = 300, eInit = 4;

        for (int i = 0; i < ledgers; i++) {
            for (int j = 0; j < entries; j++) {
                ByteBuf entry = Unpooled.buffer(1024);
                long lid = lInit + i;
                long eid = eInit + j;
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeBytes(("entry-" + eid).getBytes());
                long location = entryLogger.addEntry(lid, entry, false);
                entryLocationIndex.addLocation(lid, eid, location);
                locations.add(Pair.of(Pair.of(lid, eid), location));
            }
        }

        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats());

        // first pos
        Pair<Pair<Long, Long>, Long> firstEntry = locations.get(0);
        long lid = firstEntry.getLeft().getLeft();
        long eid = firstEntry.getLeft().getRight();
        long location = firstEntry.getRight();

        ByteBuf entry = readAheadManager.readEntryOrWait(lid, eid + entries - 2); // miss
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid + entries - 2, entry.getLong(8));

        entry = readAheadManager.readEntryOrWait(lid, eid + entries - 1); // hit
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid + entries - 1, entry.getLong(8));
    }

    @Test
    public void testReadLastEntry() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        ByteBuf entry = Unpooled.buffer(1024);
        long lid = 3;
        long eid = 4;
        entry.writeLong(lid); // ledger id
        entry.writeLong(eid); // entry id
        entry.writeBytes(("entry-" + eid).getBytes());
        long location = entryLogger.addEntry(lid, entry, false);
        entryLocationIndex.addLocation(lid, eid, location);

        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats());

        assertNull(cache.get(lid, eid));
        entry = readAheadManager.readEntryOrWait(lid, eid);
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid, entry.getLong(8));
    }

    @Test
    public void testCanSkipCachedEntriesWhenReadingAhead() throws Exception {
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        EntryLocationIndex entryLocationIndex = singleDirStorage.getEntryLocationIndex();
        ReadCache cache = singleDirStorage.getReadCache();

        // write some entries
        List<Pair<Pair<Long, Long>, Long>> locations = new ArrayList<>();
        final int ledgers = 2, lInit = 3;
        final int entries = 300, eInit = 4;

        for (int i = 0; i < ledgers; i++) {
            for (int j = 0; j < entries; j++) {
                ByteBuf entry = Unpooled.buffer(1024);
                long lid = lInit + i;
                long eid = eInit + j;
                entry.writeLong(lid); // ledger id
                entry.writeLong(eid); // entry id
                entry.writeBytes(("entry-" + eid).getBytes());
                long location = entryLogger.addEntry(lid, entry, false);
                entryLocationIndex.addLocation(lid, eid, location);
                locations.add(Pair.of(Pair.of(lid, eid), location));
            }
        }

        final long maxEntries = 100;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setProperty(ReadAheadManager.READ_AHEAD_MAX_MESSAGES, maxEntries);
        conf.setProperty(ReadAheadManager.READ_AHEAD_TASK_POOL_SIZE, 1);
        ReadAheadManager readAheadManager = new ReadAheadManager(
                entryLogger, entryLocationIndex, cache, singleDirStorage.getDbLedgerStorageStats(), conf);

        // first pos
        Pair<Pair<Long, Long>, Long> firstEntry = locations.get(0);
        long lid = firstEntry.getLeft().getLeft();
        long eid = firstEntry.getLeft().getRight();
        long location = firstEntry.getRight();

        // set init pos
        readAheadManager.readEntryOrWait(lid, eid); // miss

        // read some entries
        int skipEntries = 5;
        for (int i = 0; i < 5; i++) {
            // do not read sequentially
            readAheadManager.readEntryOrWait(lid, eid + maxEntries - i);
        }

        // start reading ahead
        ByteBuf entry = readAheadManager.readEntryOrWait(lid, eid + 1);
        assertEquals(lid, entry.getLong(0));
        assertEquals(eid + 1, entry.getLong(8));

        // check last entry pos
        ReadAheadManager.ReadAheadTaskStatus taskStatus = readAheadManager.getNearestTask(lid, eid + 1);
        while (taskStatus.getEndEntryId() < 0) {
            Thread.sleep(100);
        }
        assertEquals(eid + 1 + maxEntries + skipEntries, taskStatus.getEndEntryId());
    }
}
