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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link DbLedgerStorage}.
 */
public class DbLedgerStorageTest {

    private DbLedgerStorage storage;
    private File tmpDir;
    private LedgerDirsManager ledgerDirsManager;

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
        BookieImpl bookie = new BookieImpl(conf);

        ledgerDirsManager = bookie.getLedgerDirsManager();
        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    @Test
    public void simple() throws Exception {
        assertEquals(false, storage.ledgerExists(3));
        try {
            storage.isFenced(3);
            fail("should have failed");
        } catch (Bookie.NoLedgerException nle) {
            // OK
        }
        assertEquals(false, storage.ledgerExists(3));
        try {
            storage.setFenced(3);
            fail("should have failed");
        } catch (Bookie.NoLedgerException nle) {
            // OK
        }
        storage.setMasterKey(3, "key".getBytes());
        try {
            storage.setMasterKey(3, "other-key".getBytes());
            fail("should have failed");
        } catch (IOException ioe) {
            assertTrue(ioe.getCause() instanceof BookieException.BookieIllegalOpException);
        }
        // setting the same key is NOOP
        storage.setMasterKey(3, "key".getBytes());
        assertEquals(true, storage.ledgerExists(3));
        assertEquals(true, storage.setFenced(3));
        assertEquals(true, storage.isFenced(3));
        assertEquals(false, storage.setFenced(3));

        storage.setMasterKey(4, "key".getBytes());
        assertEquals(false, storage.isFenced(4));
        assertEquals(true, storage.ledgerExists(4));

        assertEquals("key", new String(storage.readMasterKey(4)));

        assertEquals(Lists.newArrayList(4L, 3L), Lists.newArrayList(storage.getActiveLedgersInRange(0, 100)));
        assertEquals(Lists.newArrayList(4L, 3L), Lists.newArrayList(storage.getActiveLedgersInRange(3, 100)));
        assertEquals(Lists.newArrayList(3L), Lists.newArrayList(storage.getActiveLedgersInRange(0, 4)));

        // Add / read entries
        ByteBuf entry = Unpooled.buffer(1024);
        entry.writeLong(4); // ledger id
        entry.writeLong(1); // entry id
        entry.writeLong(0); // lac
        entry.writeBytes("entry-1".getBytes());

        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());

        assertEquals(1, storage.addEntry(entry));

        assertEquals(true, ((DbLedgerStorage) storage).isFlushRequired());

        // Read from write cache
        ByteBuf res = storage.getEntry(4, 1);
        assertEquals(entry, res);

        storage.flush();

        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());

        // Read from db
        res = storage.getEntry(4, 1);
        assertEquals(entry, res);

        try {
            storage.getEntry(4, 2);
            fail("Should have thrown exception");
        } catch (NoEntryException e) {
            // ok
        }

        ByteBuf entry2 = Unpooled.buffer(1024);
        entry2.writeLong(4); // ledger id
        entry2.writeLong(2); // entry id
        entry2.writeLong(1); // lac
        entry2.writeBytes("entry-2".getBytes());

        storage.addEntry(entry2);

        // Read last entry in ledger
        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry2, res);

        // Read last add confirmed in ledger
        assertEquals(1L, storage.getLastAddConfirmed(4));

        ByteBuf entry3 = Unpooled.buffer(1024);
        entry3.writeLong(4); // ledger id
        entry3.writeLong(3); // entry id
        entry3.writeLong(2); // lac
        entry3.writeBytes("entry-3".getBytes());
        storage.addEntry(entry3);

        ByteBuf entry4 = Unpooled.buffer(1024);
        entry4.writeLong(4); // ledger id
        entry4.writeLong(4); // entry id
        entry4.writeLong(3); // lac
        entry4.writeBytes("entry-4".getBytes());
        storage.addEntry(entry4);

        res = storage.getEntry(4, 4);
        assertEquals(entry4, res);

        assertEquals(3, storage.getLastAddConfirmed(4));

        // Delete
        assertEquals(true, storage.ledgerExists(4));
        storage.deleteLedger(4);
        assertEquals(false, storage.ledgerExists(4));

        // Should not throw exception event if the ledger was deleted
        storage.getEntry(4, 4);
        assertEquals(3, storage.getLastAddConfirmed(4));

        storage.addEntry(Unpooled.wrappedBuffer(entry2));
        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry4, res);
        assertEquals(3, storage.getLastAddConfirmed(4));

        // Get last entry from storage
        storage.flush();

        try {
            storage.getEntry(4, 4);
            fail("Should have thrown exception since the ledger was deleted");
        } catch (NoEntryException e) {
            // ok
        }
    }

    @Test
    public void testBookieCompaction() throws Exception {
        storage.setMasterKey(4, "key".getBytes());

        ByteBuf entry3 = Unpooled.buffer(1024);
        entry3.writeLong(4); // ledger id
        entry3.writeLong(3); // entry id
        entry3.writeBytes("entry-3".getBytes());
        storage.addEntry(entry3);

        // Simulate bookie compaction
        SingleDirectoryDbLedgerStorage singleDirStorage = ((DbLedgerStorage) storage).getLedgerStorageList().get(0);
        EntryLogger entryLogger = singleDirStorage.getEntryLogger();
        // Rewrite entry-3
        ByteBuf newEntry3 = Unpooled.buffer(1024);
        newEntry3.writeLong(4); // ledger id
        newEntry3.writeLong(3); // entry id
        newEntry3.writeBytes("new-entry-3".getBytes());
        long location = entryLogger.addEntry(4L, newEntry3, false);

        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(4, 3, location));
        singleDirStorage.updateEntriesLocations(locations);

        ByteBuf res = storage.getEntry(4, 3);
        System.out.println("res:       " + ByteBufUtil.hexDump(res));
        System.out.println("newEntry3: " + ByteBufUtil.hexDump(newEntry3));
        assertEquals(newEntry3, res);
    }

    @Test
    public void doubleDirectory() throws Exception {
        int gcWaitTime = 1000;
        File firstDir = new File(tmpDir, "dir1");
        File secondDir = new File(tmpDir, "dir2");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { firstDir.getCanonicalPath(), secondDir.getCanonicalPath() });

        // Should not fail
        Bookie bookie = new BookieImpl(conf);
        assertEquals(2, ((DbLedgerStorage) bookie.getLedgerStorage()).getLedgerStorageList().size());

        bookie.shutdown();
    }

    @Test
    public void testRewritingEntries() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        try {
            storage.getEntry(1, -1);
            fail("Should throw exception");
        } catch (Bookie.NoEntryException e) {
            // ok
        }

        ByteBuf entry1 = Unpooled.buffer(1024);
        entry1.writeLong(1); // ledger id
        entry1.writeLong(1); // entry id
        entry1.writeBytes("entry-1".getBytes());

        storage.addEntry(entry1);
        storage.flush();

        ByteBuf newEntry1 = Unpooled.buffer(1024);
        newEntry1.writeLong(1); // ledger id
        newEntry1.writeLong(1); // entry id
        newEntry1.writeBytes("new-entry-1".getBytes());

        storage.addEntry(newEntry1);
        storage.flush();

        ByteBuf response = storage.getEntry(1, 1);
        assertEquals(newEntry1, response);
    }

    @Test
    public void testEntriesOutOfOrder() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        ByteBuf entry2 = Unpooled.buffer(1024);
        entry2.writeLong(1); // ledger id
        entry2.writeLong(2); // entry id
        entry2.writeBytes("entry-2".getBytes());

        storage.addEntry(entry2);

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (NoEntryException e) {
            // Ok, entry doesn't exist
        }

        ByteBuf res = storage.getEntry(1, 2);
        assertEquals(entry2, res);

        ByteBuf entry1 = Unpooled.buffer(1024);
        entry1.writeLong(1); // ledger id
        entry1.writeLong(1); // entry id
        entry1.writeBytes("entry-1".getBytes());

        storage.addEntry(entry1);

        res = storage.getEntry(1, 1);
        assertEquals(entry1, res);

        res = storage.getEntry(1, 2);
        assertEquals(entry2, res);

        storage.flush();

        res = storage.getEntry(1, 1);
        assertEquals(entry1, res);

        res = storage.getEntry(1, 2);
        assertEquals(entry2, res);
    }

    @Test
    public void testEntriesOutOfOrderWithFlush() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        ByteBuf entry2 = Unpooled.buffer(1024);
        entry2.writeLong(1); // ledger id
        entry2.writeLong(2); // entry id
        entry2.writeBytes("entry-2".getBytes());

        storage.addEntry(entry2);

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (NoEntryException e) {
            // Ok, entry doesn't exist
        }

        ByteBuf res = storage.getEntry(1, 2);
        assertEquals(entry2, res);
        res.release();

        storage.flush();

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (NoEntryException e) {
            // Ok, entry doesn't exist
        }

        res = storage.getEntry(1, 2);
        assertEquals(entry2, res);
        res.release();

        ByteBuf entry1 = Unpooled.buffer(1024);
        entry1.writeLong(1); // ledger id
        entry1.writeLong(1); // entry id
        entry1.writeBytes("entry-1".getBytes());

        storage.addEntry(entry1);

        res = storage.getEntry(1, 1);
        assertEquals(entry1, res);
        res.release();

        res = storage.getEntry(1, 2);
        assertEquals(entry2, res);
        res.release();

        storage.flush();

        res = storage.getEntry(1, 1);
        assertEquals(entry1, res);
        res.release();

        res = storage.getEntry(1, 2);
        assertEquals(entry2, res);
        res.release();
    }

    @Test
    public void testAddEntriesAfterDelete() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        ByteBuf entry0 = Unpooled.buffer(1024);
        entry0.writeLong(1); // ledger id
        entry0.writeLong(0); // entry id
        entry0.writeBytes("entry-0".getBytes());

        ByteBuf entry1 = Unpooled.buffer(1024);
        entry1.writeLong(1); // ledger id
        entry1.writeLong(1); // entry id
        entry1.writeBytes("entry-1".getBytes());

        storage.addEntry(entry0);
        storage.addEntry(entry1);

        storage.flush();

        storage.deleteLedger(1);

        storage.setMasterKey(1, "key".getBytes());

        entry0 = Unpooled.buffer(1024);
        entry0.writeLong(1); // ledger id
        entry0.writeLong(0); // entry id
        entry0.writeBytes("entry-0".getBytes());

        entry1 = Unpooled.buffer(1024);
        entry1.writeLong(1); // ledger id
        entry1.writeLong(1); // entry id
        entry1.writeBytes("entry-1".getBytes());

        storage.addEntry(entry0);
        storage.addEntry(entry1);

        assertEquals(entry0, storage.getEntry(1, 0));
        assertEquals(entry1, storage.getEntry(1, 1));

        storage.flush();
    }

    @Test
    public void testGetLedgerDirsListeners() throws IOException {
        // we should have two listeners, one is the SingleLedgerDirectories listener,
        // and another is EntryLogManagerForEntryLogPerLedger
        assertEquals(2, ledgerDirsManager.getListeners().size());
    }
}
