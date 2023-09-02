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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookie journal.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JournalChannel.class, FileChannelProvider.class})
@PowerMockIgnore({"jdk.internal.loader.*", "javax.xml.*", "org.xml.*", "org.w3c.*",
    "com.sun.org.apache.xerces.*", "javax.naming.*"})
public class BookieJournalTest {
    private static final Logger LOG = LoggerFactory.getLogger(BookieJournalTest.class);

    final Random r = new Random(System.currentTimeMillis());

    final List<File> tempDirs = new ArrayList<File>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    private void writeIndexFileForLedger(File indexDir, long ledgerId,
                                         byte[] masterKey)
            throws Exception {
        File fn = new File(indexDir, IndexPersistenceMgr.getLedgerName(ledgerId));
        fn.getParentFile().mkdirs();
        FileInfo fi = new FileInfo(fn, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        // force creation of index file
        fi.write(new ByteBuffer[]{ ByteBuffer.allocate(0) }, 0);
        fi.close(true);
    }

    private void writePartialIndexFileForLedger(File indexDir, long ledgerId,
                                                byte[] masterKey, boolean truncateToMasterKey)
            throws Exception {
        File fn = new File(indexDir, IndexPersistenceMgr.getLedgerName(ledgerId));
        fn.getParentFile().mkdirs();
        FileInfo fi = new FileInfo(fn, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        // force creation of index file
        fi.write(new ByteBuffer[]{ ByteBuffer.allocate(0) }, 0);
        fi.close(true);
        // file info header
        int headerLen = 8 + 4 + masterKey.length;
        // truncate the index file
        int leftSize;
        if (truncateToMasterKey) {
            leftSize = r.nextInt(headerLen);
        } else {
            leftSize = headerLen + r.nextInt(1024 - headerLen);
        }
        FileChannel fc = new RandomAccessFile(fn, "rw").getChannel();
        fc.truncate(leftSize);
        fc.close();
    }

    /**
     * Generate fence entry.
     */
    private static ByteBuf generateFenceEntry(long ledgerId) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_FENCE_KEY);
        return bb;
    }

    /**
     * Generate meta entry with given master key.
     */
    private static ByteBuf generateMetaEntry(long ledgerId, byte[] masterKey) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_LEDGER_KEY);
        bb.writeInt(masterKey.length);
        bb.writeBytes(masterKey);
        return bb;
    }

    private void writeJunkJournal(File journalDir) throws Exception {
        long logId = System.currentTimeMillis();
        File fn = new File(journalDir, Long.toHexString(logId) + ".txn");

        FileChannel fc = new RandomAccessFile(fn, "rw").getChannel();

        ByteBuffer zeros = ByteBuffer.allocate(512);
        fc.write(zeros, 4 * 1024 * 1024);
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
        fc.write(zeros, 4 * 1024 * 1024);
        fc.position(0);

        byte[] data = "JournalTestData".getBytes();
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 1; i <= numEntries; i++) {
            ByteBuf packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();

            fc.write(lenBuff);
            fc.write(packet.nioBuffer());
            ReferenceCountUtil.release(packet);
        }
    }

    private static void moveToPosition(JournalChannel jc, long pos) throws IOException {
        jc.fc.position(pos);
        jc.bc.position = pos;
        jc.bc.writeBufferStartPosition.set(pos);
    }

    private static void updateJournalVersion(JournalChannel jc, int journalVersion) throws IOException {
        long prevPos = jc.fc.position();
        try {
            ByteBuffer versionBuffer = ByteBuffer.allocate(4);
            versionBuffer.putInt(journalVersion);
            versionBuffer.flip();
            jc.fc.position(4);
            IOUtils.writeFully(jc.fc, versionBuffer);
            jc.fc.force(true);
        } finally {
            jc.fc.position(prevPos);
        }
    }

    private JournalChannel writeV2Journal(File journalDir, int numEntries) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 1; i <= numEntries; i++) {
            ByteBuf packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();

            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            ReferenceCountUtil.release(packet);
        }
        bc.flushAndForceWrite(false);

        updateJournalVersion(jc, JournalChannel.V2);

        return jc;
    }

    private JournalChannel writeV3Journal(File journalDir, int numEntries, byte[] masterKey) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            }
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();

            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            ReferenceCountUtil.release(packet);
        }
        bc.flushAndForceWrite(false);

        updateJournalVersion(jc, JournalChannel.V3);

        return jc;
    }

    private JournalChannel writeV4Journal(File journalDir, int numEntries, byte[] masterKey) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            }
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();
            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            ReferenceCountUtil.release(packet);
        }
        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        bc.write(lenBuf);
        bc.write(packet);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V4);
        return jc;
    }

    private JournalChannel writeV4JournalWithInvalidRecord(File journalDir,
                                                           int numEntries, byte[] masterKey) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            }
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            if (i == numEntries - 1) {
                //mock when flush data to file ,it writes an invalid entry to journal
                lenBuff.putInt(-1);
            } else {
                lenBuff.putInt(packet.readableBytes());
            }
            lenBuff.flip();
            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            packet.release();
        }

        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        //mock
        bc.write(lenBuf);
        bc.write(packet);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V4);

        return jc;
    }

    static JournalChannel writeV5Journal(File journalDir, int numEntries,
                                         byte[] masterKey) throws Exception {
        return writeV5Journal(journalDir, numEntries, masterKey, false);
    }

    static JournalChannel writeV5Journal(File journalDir, int numEntries,
                                         byte[] masterKey, boolean corruptLength) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        BufferedChannel bc = jc.getBufferedChannel();

        ByteBuf paddingBuff = Unpooled.buffer();
        paddingBuff.writeZero(2 * JournalChannel.SECTOR_SIZE);
        byte[] data = new byte[4 * 1024 * 1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        long length = 0;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = ClientUtil.generatePacket(1, i, lastConfirmed, length, data, 0, i);
            }
            lastConfirmed = i;
            length += i;
            ByteBuf lenBuff = Unpooled.buffer();
            if (corruptLength) {
                lenBuff.writeInt(-1);
            } else {
                lenBuff.writeInt(packet.readableBytes());
            }
            bc.write(lenBuff);
            bc.write(packet);
            ReferenceCountUtil.release(packet);
            Journal.writePaddingBytes(jc, paddingBuff, JournalChannel.SECTOR_SIZE);
        }
        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        bc.write(lenBuf);
        bc.write(packet);
        Journal.writePaddingBytes(jc, paddingBuff, JournalChannel.SECTOR_SIZE);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V5);
        return jc;
    }

    /**
     * test that we can open a journal written without the magic
     * word at the start. This is for versions of bookkeeper before
     * the magic word was introduced
     */
    @Test
    public void testPreV2Journal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writePreV2Journal(BookieImpl.getCurrentDirectory(journalDir), 100);
        writeIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir), 1, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = createBookieAndReadJournal(conf);

        b.readEntry(1, 100);
        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }

        b.shutdown();
    }

    @Test
    public void testV4Journal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        BookieImpl b = createBookieAndReadJournal(conf);

        b.readEntry(1, 100);
        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }
        assertTrue(b.handles.getHandle(1, "testPasswd".getBytes()).isFenced());

        b.shutdown();
    }

    @Test
    public void testV5Journal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writeV5Journal(BookieImpl.getCurrentDirectory(journalDir), 2 * JournalChannel.SECTOR_SIZE,
                "testV5Journal".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        BookieImpl b = createBookieAndReadJournal(conf);

        for (int i = 1; i <= 2 * JournalChannel.SECTOR_SIZE; i++) {
            b.readEntry(1, i);
        }
        try {
            b.readEntry(1, 2 * JournalChannel.SECTOR_SIZE + 1);
            fail("Shouldn't have found entry " + (2 * JournalChannel.SECTOR_SIZE + 1));
        } catch (Bookie.NoEntryException e) {
            // correct behavior
        }
        assertTrue(b.handles.getHandle(1, "testV5Journal".getBytes()).isFenced());

        b.shutdown();
    }

    /**
     * Test that if the journal is all journal, we can not
     * start the bookie. An admin should look to see what has
     * happened in this case
     */
    @Test
    public void testAllJunkJournal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writeJunkJournal(BookieImpl.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = null;
        try {
            b = new TestBookieImpl(conf);
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
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writePreV2Journal(BookieImpl.getCurrentDirectory(journalDir), 0);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = new TestBookieImpl(conf);
    }

    /**
     * Test that a journal can load if only the magic word and
     * version are there.
     */
    @Test
    public void testHeaderOnlyJournal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writeV2Journal(BookieImpl.getCurrentDirectory(journalDir), 0);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = new TestBookieImpl(conf);
    }

    /**
     * Test that if a journal has junk at the end, it does not load.
     * If the journal is corrupt like this, admin intervention is needed
     */
    @Test
    public void testJunkEndedJournal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV2Journal(BookieImpl.getCurrentDirectory(journalDir), 0);
        jc.getBufferedChannel().write(Unpooled.wrappedBuffer("JunkJunkJunk".getBytes()));
        jc.getBufferedChannel().flushAndForceWrite(false);
        jc.close();

        writeIndexFileForLedger(ledgerDir, 1, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = null;
        try {
            b = new TestBookieImpl(conf);
        } catch (Throwable t) {
            // correct behaviour
        }
    }

    /**
     * Test that if the bookie crashes while writing the length
     * of an entry, that we can recover.
     *
     * <p>This is currently not the case, which is bad as recovery
     * should be fine here. The bookie has crashed while writing
     * but so the client has not be notified of success.
     */
    @Test
    public void testTruncatedInLenJournal() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV2Journal(
                BookieImpl.getCurrentDirectory(journalDir), 100);
        ByteBuffer zeros = ByteBuffer.allocate(2048);

        jc.fc.position(jc.getBufferedChannel().position() - 0x429);
        jc.fc.write(zeros);
        jc.fc.force(false);

        writeIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir),
                                1, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = createBookieAndReadJournal(conf);

        b.readEntry(1, 99);

        try {
            b.readEntry(1, 100);
            fail("Shouldn't have found entry 100");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }
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
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV2Journal(
                BookieImpl.getCurrentDirectory(journalDir), 100);
        ByteBuffer zeros = ByteBuffer.allocate(2048);

        jc.fc.position(jc.getBufferedChannel().position() - 0x300);
        jc.fc.write(zeros);
        jc.fc.force(false);

        writeIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir),
                                1, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        Bookie b = createBookieAndReadJournal(conf);

        b.readEntry(1, 99);

        // still able to read last entry, but it's junk
        ByteBuf buf = b.readEntry(1, 100);
        assertEquals("Ledger Id is wrong", buf.readLong(), 1);
        assertEquals("Entry Id is wrong", buf.readLong(), 100);
        assertEquals("Last confirmed is wrong", buf.readLong(), 99);
        assertEquals("Length is wrong", buf.readLong(), 100 * 1024);
        buf.readLong(); // skip checksum
        boolean allX = true;
        for (int i = 0; i < 1024; i++) {
            byte x = buf.readByte();
            allX = allX && x == (byte) 'X';
        }
        assertFalse("Some of buffer should have been zeroed", allX);

        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }
    }

    private BookieImpl createBookieAndReadJournal(ServerConfiguration conf) throws Exception {
        BookieImpl b = new TestBookieImpl(conf);
        for (Journal journal : b.journals) {
            LastLogMark lastLogMark = journal.getLastLogMark().markLog();
            b.readJournal();
            assertTrue(journal.getLastLogMark().getCurMark().compare(lastLogMark.getCurMark()) > 0);
        }
        return b;
    }

    /**
     * Test journal replay with SortedLedgerStorage and a very small max
     * arena size.
     */
    @Test
    public void testSortedLedgerStorageReplayWithSmallMaxArenaSize() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV2Journal(
                BookieImpl.getCurrentDirectory(journalDir), 100);

        jc.fc.force(false);

        writeIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir),
                1, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerStorageClass("org.apache.bookkeeper.bookie.SortedLedgerStorage");
        conf.setSkipListArenaMaxAllocSize(0);
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() });

        BookieImpl b = new TestBookieImpl(conf);
        b.readJournal();
        b.ledgerStorage.flush();
        b.readEntry(1, 80);
        b.readEntry(1, 99);
    }

    /**
     * Test partial index (truncate master key) with pre-v3 journals.
     */
    @Test
    public void testPartialFileInfoPreV3Journal1() throws Exception {
        testPartialFileInfoPreV3Journal(true);
    }

    /**
     * Test partial index with pre-v3 journals.
     */
    @Test
    public void testPartialFileInfoPreV3Journal2() throws Exception {
        testPartialFileInfoPreV3Journal(false);
    }

    /**
     * Test partial index file with pre-v3 journals.
     */
    private void testPartialFileInfoPreV3Journal(boolean truncateMasterKey)
        throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writePreV2Journal(BookieImpl.getCurrentDirectory(journalDir), 100);
        writePartialIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir),
                                       1, "testPasswd".getBytes(), truncateMasterKey);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        if (truncateMasterKey) {
            try {
                BookieImpl b = new TestBookieImpl(conf);
                b.readJournal();
                fail("Should not reach here!");
            } catch (IOException ie) {
            }
        } else {
            BookieImpl b = new TestBookieImpl(conf);
            b.readJournal();
            b.readEntry(1, 100);
            try {
                b.readEntry(1, 101);
                fail("Shouldn't have found entry 101");
            } catch (Bookie.NoEntryException e) {
                // correct behaviour
            }
        }
    }

    /**
     * Test partial index (truncate master key) with post-v3 journals.
     */
    @Test
    public void testPartialFileInfoPostV3Journal1() throws Exception {
        testPartialFileInfoPostV3Journal(true);
    }

    /**
     * Test partial index with post-v3 journals.
     */
    @Test
    public void testPartialFileInfoPostV3Journal2() throws Exception {
        testPartialFileInfoPostV3Journal(false);
    }

    /**
     * Test partial index file with post-v3 journals.
     */
    private void testPartialFileInfoPostV3Journal(boolean truncateMasterKey)
        throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        byte[] masterKey = "testPasswd".getBytes();

        writeV3Journal(BookieImpl.getCurrentDirectory(journalDir), 100, masterKey);
        writePartialIndexFileForLedger(BookieImpl.getCurrentDirectory(ledgerDir), 1, masterKey,
                                       truncateMasterKey);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setMetadataServiceUri(null);

        BookieImpl b = new TestBookieImpl(conf);
        b.readJournal();
        b.readEntry(1, 100);
        try {
            b.readEntry(1, 101);
            fail("Shouldn't have found entry 101");
        } catch (Bookie.NoEntryException e) {
            // correct behaviour
        }
    }

    /**
      * Test for fake IOException during read of Journal.
      */
    @Test
    public void testJournalScanIOException() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), 100, "testPasswd".getBytes());

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        Journal.JournalScanner journalScanner = new DummyJournalScan();
        BookieFileChannel bookieFileChannel = PowerMockito.mock(BookieFileChannel.class);
        FileChannel fileChannel = PowerMockito.mock(FileChannel.class);
        FileChannelProvider fileChannelProvider = PowerMockito.mock(FileChannelProvider.class);

        PowerMockito.when(fileChannel.position(Mockito.anyLong()))
                .thenThrow(new IOException());

        PowerMockito.mockStatic(FileChannelProvider.class);
        PowerMockito.when(FileChannelProvider.newProvider(Mockito.any())).thenReturn(fileChannelProvider);
        Mockito.when(fileChannelProvider.open(Mockito.any(), Mockito.any())).thenReturn(bookieFileChannel);
        Mockito.when(bookieFileChannel.getFileChannel()).thenReturn(fileChannel);

        BookieImpl b = new TestBookieImpl(conf);

        for (Journal journal : b.journals) {
            List<Long> journalIds = journal.listJournalIds(journal.getJournalDirectory(), null);

            assertEquals(journalIds.size(), 1);

            try {
                journal.scanJournal(journalIds.get(0), Long.MAX_VALUE, journalScanner, false);
                fail("Should not have been able to scan the journal");
            } catch (Exception e) {
                // Expected
            }
        }

        b.shutdown();
    }

    /**
     * Test for invalid record data during read of Journal.
     */
    @Test
    public void testJournalScanInvalidRecordWithSkipFlag() throws Exception {
        File journalDir = createTempDir("bookie", "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        File ledgerDir = createTempDir("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        try {
            writeV4JournalWithInvalidRecord(BookieImpl.getCurrentDirectory(journalDir),
                100, "testPasswd".getBytes());
        } catch (Exception e) {
            fail();
        }


        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        // Disabled skip broken journal files by default
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null)
                .setSkipReplayJournalInvalidRecord(true);

        Journal.JournalScanner journalScanner = new DummyJournalScan();

        BookieImpl b = new TestBookieImpl(conf);

        for (Journal journal : b.journals) {
            List<Long> journalIds = Journal.listJournalIds(journal.getJournalDirectory(), null);
            assertEquals(journalIds.size(), 1);
            try {
                journal.scanJournal(journalIds.get(0), 0, journalScanner, conf.isSkipReplayJournalInvalidRecord());
            } catch (Exception e) {
                fail("Should pass the journal scanning because we enabled skip flag by default.");
            }
        }

        b.shutdown();

        // Disabled skip broken journal files by default
        conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        journalScanner = new DummyJournalScan();

        b = new TestBookieImpl(conf);

        for (Journal journal : b.journals) {
            List<Long> journalIds = Journal.listJournalIds(journal.getJournalDirectory(), null);
            assertEquals(journalIds.size(), 1);
            try {
                journal.scanJournal(journalIds.get(0), 0, journalScanner, conf.isSkipReplayJournalInvalidRecord());
                fail("Should fail the journal scanning because of disabled skip flag");
            } catch (Exception e) {
                // expected.
            }
        }

        b.shutdown();
    }


    static class DummyJournalScan implements Journal.JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
            LOG.warn("Journal Version : " + journalVersion);
        }
    };
}
