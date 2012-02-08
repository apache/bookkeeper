package org.apache.bookkeeper.bookie;

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

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Random;
import java.util.Set;
import java.util.Arrays;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class BookieJournalTest {
    static Logger LOG = LoggerFactory.getLogger(BookieJournalTest.class);

    private void writeIndexFileForLedger(File indexDir, long ledgerId,
                                         byte[] masterKey)
            throws Exception {
        File fn = new File(indexDir, LedgerCache.getLedgerName(ledgerId));
        fn.getParentFile().mkdirs();
        FileInfo fi = new FileInfo(fn);
        fi.writeMasterKey(masterKey);
        fi.close();
    }

    private void writeJunkJournal(File journalDir) throws Exception {
        long logId = System.currentTimeMillis();
        File fn = new File(journalDir, Long.toHexString(logId) + ".txn");

        FileChannel fc = new RandomAccessFile(fn, "rw").getChannel();

        ByteBuffer zeros = ByteBuffer.allocate(512);
        fc.write(zeros, 4*1024*1024);
        fc.position(0);

        for (int i = 1; i <= 10; i++) {
            fc.write(ByteBuffer.wrap("JunkJunkJunk".getBytes()));
        }
    }

    private void writePreV2Journal(File journalDir, int numEntries) throws Exception {
        long logId = System.currentTimeMillis();
        File fn = new File(journalDir, Long.toHexString(logId) + ".txn");

        FileChannel fc = new RandomAccessFile(fn, "rw").getChannel();

        ByteBuffer zeros = ByteBuffer.allocate(512);
        fc.write(zeros, 4*1024*1024);
        fc.position(0);

        byte[] data = "JournalTestData".getBytes();
        long lastConfirmed = -1;
        for (int i = 1; i <= numEntries; i++) {
            ByteBuffer packet = ClientUtil.generatePacket(1, i, lastConfirmed, i*data.length, data).toByteBuffer();
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.remaining());
            lenBuff.flip();

            fc.write(lenBuff);
            fc.write(packet);
        }
    }

    private JournalChannel writePostV2Journal(File journalDir, int numEntries) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'X');
        long lastConfirmed = -1;
        for (int i = 1; i <= numEntries; i++) {
            ByteBuffer packet = ClientUtil.generatePacket(1, i, lastConfirmed, i*data.length, data).toByteBuffer();
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.remaining());
            lenBuff.flip();

            bc.write(lenBuff);
            bc.write(packet);
        }
        bc.flush(true);

        return jc;
    }

    /**
     * test that we can open a journal written without the magic
     * word at the start. This is for versions of bookkeeper before
     * the magic word was introduced
     */
    @Test
    public void testPreV2Journal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        writePreV2Journal(journalDir, 100);
        writeIndexFileForLedger(ledgerDir, 1, "testPasswd".getBytes());

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = new Bookie(conf);

        b.readEntry(1, 100);
        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }

        b.shutdown();
    }

    /**
     * Test that if the journal is all journal, we can not
     * start the bookie. An admin should look to see what has
     * happened in this case
     */
    @Test
    public void testAllJunkJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        writeJunkJournal(journalDir);

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });
        Bookie b = null;
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start without admin");
        } catch (Throwable t) {
            // correct behaviour
        } finally {
            if (b != null) {
                b.shutdown();
            }
        }
    }

    /**
     * Test that we can start with an empty journal.
     * This can happen if the bookie crashes between creating the
     * journal and writing the magic word. It could also happen before
     * the magic word existed, if the bookie started but nothing was
     * ever written.
     */
    @Test
    public void testEmptyJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        writePreV2Journal(journalDir, 0);

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = new Bookie(conf);
        b.shutdown();
    }

    /**
     * Test that a journal can load if only the magic word and
     * version are there.
     */
    @Test
    public void testHeaderOnlyJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        writePostV2Journal(journalDir, 0);

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = new Bookie(conf);
        b.shutdown();
    }

    /**
     * Test that if a journal has junk at the end, it does not load.
     * If the journal is corrupt like this, admin intervention is needed
     */
    @Test
    public void testJunkEndedJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        JournalChannel jc = writePostV2Journal(journalDir, 0);
        jc.getBufferedChannel().write(ByteBuffer.wrap("JunkJunkJunk".getBytes()));
        jc.getBufferedChannel().flush(true);

        writeIndexFileForLedger(ledgerDir, 1, "testPasswd".getBytes());

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = null;
        try {
            b = new Bookie(conf);
        } catch (Throwable t) {
            // correct behaviour
        } finally {
            if (b != null) {
                b.shutdown();
            }
        }
    }

    /**
     * Test that if the bookie crashes while writing the length
     * of an entry, that we can recover.
     *
     * This is currently not the case, which is bad as recovery
     * should be fine here. The bookie has crashed while writing
     * but so the client has not be notified of success.
     */
    //    @Test TODO, fix and reenable
    public void testTruncatedInLenJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        JournalChannel jc = writePostV2Journal(journalDir, 100);
        ByteBuffer zeros = ByteBuffer.allocate(2048);

        jc.fc.position(jc.getBufferedChannel().position() - 0x429);
        jc.fc.write(zeros);
        jc.fc.force(false);

        writeIndexFileForLedger(ledgerDir, 1, "testPasswd".getBytes());

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = new Bookie(conf);

        b.readEntry(1, 99);

        try {
            b.readEntry(1, 100);
            fail("Shouldn't have found entry 100");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }

        b.shutdown();
    }

    /**
     * Test that if the bookie crashes in the middle of writing
     * the actual entry it can recover.
     * In this case the entry will be available, but it will corrupt.
     * This is ok, as the client will disregard the entry after looking
     * at its checksum.
     */
    @Test
    public void testTruncatedInEntryJournal() throws Exception {
        File journalDir = File.createTempFile("bookie", "journal");
        journalDir.delete();
        journalDir.mkdir();

        File ledgerDir = File.createTempFile("bookie", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();

        JournalChannel jc = writePostV2Journal(journalDir, 100);
        ByteBuffer zeros = ByteBuffer.allocate(2048);

        jc.fc.position(jc.getBufferedChannel().position() - 0x300);
        jc.fc.write(zeros);
        jc.fc.force(false);

        writeIndexFileForLedger(ledgerDir, 1, "testPasswd".getBytes());

        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(null)
            .setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        Bookie b = new Bookie(conf);
        b.readEntry(1, 99);

        // still able to read last entry, but it's junk
        ByteBuffer buf = b.readEntry(1, 100);
        assertEquals("Ledger Id is wrong", buf.getLong(), 1);
        assertEquals("Entry Id is wrong", buf.getLong(), 100);
        assertEquals("Last confirmed is wrong", buf.getLong(), 99);
        assertEquals("Length is wrong", buf.getLong(), 100*1024);
        buf.getLong(); // skip checksum
        boolean allX = true;
        for (int i = 0; i < 1024; i++) {
            byte x = buf.get();
            allX = allX && x == (byte)'X';
        }
        assertFalse("Some of buffer should have been zeroed", allX);

        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }

        b.shutdown();
    }

}
