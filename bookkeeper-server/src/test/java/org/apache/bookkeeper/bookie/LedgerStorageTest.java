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

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ledger storage.
 */
public class LedgerStorageTest extends BookKeeperClusterTestCase {
    public LedgerStorageTest() {
        super(1);
    }

    @Test
    public void testLedgerDeleteNotification() throws Exception {
        LedgerStorage ledgerStorage = serverByIndex(0).getBookie().getLedgerStorage();

        long deletedLedgerId = 5;
        ledgerStorage.setMasterKey(deletedLedgerId, new byte[0]);

        CountDownLatch counter = new CountDownLatch(1);

        ledgerStorage.registerLedgerDeletionListener(ledgerId -> {
            assertEquals(deletedLedgerId, ledgerId);

            counter.countDown();
        });

        ledgerStorage.deleteLedger(deletedLedgerId);

        counter.await();
    }

    @Test
    public void testExplicitLacWriteToJournalWithValidVersions() throws Exception {
        /*
         * to persist explicitLac, journalFormatVersionToWrite should be atleast
         * V6 and fileInfoFormatVersionToWrite should be atleast V1
         */
        testExplicitLacWriteToJournal(6, 1);
    }

    @Test
    public void testExplicitLacWriteToJournalWithOlderVersions() throws Exception {
        /*
         * to persist explicitLac, journalFormatVersionToWrite should be atleast
         * V6 and fileInfoFormatVersionToWrite should be atleast V1
         */
        testExplicitLacWriteToJournal(5, 0);
    }

    public void testExplicitLacWriteToJournal(int journalFormatVersionToWrite, int fileInfoFormatVersionToWrite)
            throws Exception {
        restartBookies(c -> {
                c.setJournalFormatVersionToWrite(journalFormatVersionToWrite);
                c.setFileInfoFormatVersionToWrite(fileInfoFormatVersionToWrite);
                return c;
            });


        ClientConfiguration confWithExplicitLAC = new ClientConfiguration();
        confWithExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        /*
         * enable explicitLacFlush by setting non-zero value for
         * explictLacInterval
         */
        int explictLacInterval = 100;
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        byte[] passwdBytes = "testPasswd".getBytes();
        confWithExplicitLAC.setExplictLacInterval(explictLacInterval);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithExplicitLAC);

        LedgerHandle wlh = bkcWithExplicitLAC.createLedger(1, 1, 1, digestType, passwdBytes);
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, passwdBytes);

        assertEquals("LAC of rlh", (long) numOfEntries - 2, rlh.getLastAddConfirmed());
        assertEquals("Read explicit LAC of rlh", (long) numOfEntries - 2, rlh.readExplicitLastConfirmed());

        /*
         * we need to wait for atleast 2 explicitlacintervals, since in
         * writehandle for the first call lh.getExplicitLastAddConfirmed() will
         * be < lh.getPiggyBackedLastAddConfirmed(), so it wont make explicit
         * writelac in the first run
         */
        long readExplicitLastConfirmed = TestUtils.waitUntilExplicitLacUpdated(rlh, numOfEntries - 1);
        assertEquals("Read explicit LAC of rlh after wait for explicitlacflush", (numOfEntries - 1),
                readExplicitLastConfirmed);

        ServerConfiguration newBookieConf = new ServerConfiguration(confByIndex(0));
        /*
         * by reusing bookieServerConfig and setting metadataServiceUri to null
         * we can create/start new Bookie instance using the same data
         * (journal/ledger/index) of the existing BookeieServer for our testing
         * purpose.
         */
        newBookieConf.setMetadataServiceUri(null);
        BookieImpl newbookie = new TestBookieImpl(newBookieConf);
        /*
         * since 'newbookie' uses the same data as original Bookie, it should be
         * able to read journal of the original bookie and hence explicitLac buf
         * entry written to Journal in the original bookie.
         */
        newbookie.readJournal();
        ByteBuf explicitLacBuf = newbookie.getExplicitLac(ledgerId);

        if ((journalFormatVersionToWrite >= 6) && (fileInfoFormatVersionToWrite >= 1)) {
            DigestManager digestManager = DigestManager.instantiate(ledgerId, passwdBytes,
                    BookKeeper.DigestType.toProtoDigestType(digestType), UnpooledByteBufAllocator.DEFAULT,
                    confWithExplicitLAC.getUseV2WireProtocol());
            long explicitLacPersistedInJournal = digestManager.verifyDigestAndReturnLac(explicitLacBuf);
            assertEquals("explicitLac persisted in journal", (numOfEntries - 1), explicitLacPersistedInJournal);
        } else {
            assertEquals("explicitLac is not expected to be persisted, so it should be null", null, explicitLacBuf);
        }
        bkcWithExplicitLAC.close();
    }

    @Test
    public void testExplicitLacWriteToFileInfoWithValidVersions() throws Exception {
        /*
         * to persist explicitLac, journalFormatVersionToWrite should be atleast
         * V6 and fileInfoFormatVersionToWrite should be atleast V1
         */
        testExplicitLacWriteToFileInfo(6, 1);
    }

    @Test
    public void testExplicitLacWriteToFileInfoWithOlderVersions() throws Exception {
        /*
         * to persist explicitLac, journalFormatVersionToWrite should be atleast
         * V6 and fileInfoFormatVersionToWrite should be atleast V1
         */
        testExplicitLacWriteToFileInfo(5, 0);
    }

    public void testExplicitLacWriteToFileInfo(int journalFormatVersionToWrite, int fileInfoFormatVersionToWrite)
            throws Exception {
        restartBookies(c -> {
                c.setJournalFormatVersionToWrite(journalFormatVersionToWrite);
                c.setFileInfoFormatVersionToWrite(fileInfoFormatVersionToWrite);
                return c;
            });

        ClientConfiguration confWithExplicitLAC = new ClientConfiguration();
        confWithExplicitLAC.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        /*
         * enable explicitLacFlush by setting non-zero value for
         * explictLacInterval
         */
        int explictLacInterval = 100;
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        byte[] passwdBytes = "testPasswd".getBytes();
        confWithExplicitLAC.setExplictLacInterval(explictLacInterval);

        BookKeeper bkcWithExplicitLAC = new BookKeeper(confWithExplicitLAC);

        LedgerHandle wlh = bkcWithExplicitLAC.createLedger(1, 1, 1, digestType, passwdBytes);
        long ledgerId = wlh.getId();
        int numOfEntries = 5;
        for (int i = 0; i < numOfEntries; i++) {
            wlh.addEntry(("foobar" + i).getBytes());
        }

        LedgerHandle rlh = bkcWithExplicitLAC.openLedgerNoRecovery(ledgerId, digestType, passwdBytes);

        assertEquals("LAC of rlh", (long) numOfEntries - 2, rlh.getLastAddConfirmed());
        assertEquals("Read explicit LAC of rlh", (long) numOfEntries - 2, rlh.readExplicitLastConfirmed());

        /*
         * we need to wait for atleast 2 explicitlacintervals, since in
         * writehandle for the first call lh.getExplicitLastAddConfirmed() will
         * be < lh.getPiggyBackedLastAddConfirmed(), so it wont make explicit
         * writelac in the first run
         */
        long readExplicitLastConfirmed = TestUtils.waitUntilExplicitLacUpdated(rlh, numOfEntries - 1);
        assertEquals("Read explicit LAC of rlh after wait for explicitlacflush", (numOfEntries - 1),
                readExplicitLastConfirmed);

        /*
         * flush ledgerStorage so that header of fileinfo is flushed.
         */
        serverByIndex(0).getBookie().getLedgerStorage().flush();

        ReadOnlyFileInfo fileInfo = getFileInfo(ledgerId,
                                                BookieImpl.getCurrentDirectories(confByIndex(0).getLedgerDirs()));
        fileInfo.readHeader();
        ByteBuf explicitLacBufReadFromFileInfo = fileInfo.getExplicitLac();

        if ((journalFormatVersionToWrite >= 6) && (fileInfoFormatVersionToWrite >= 1)) {
            DigestManager digestManager = DigestManager.instantiate(ledgerId, passwdBytes,
                    BookKeeper.DigestType.toProtoDigestType(digestType), UnpooledByteBufAllocator.DEFAULT,
                    confWithExplicitLAC.getUseV2WireProtocol());
            long explicitLacReadFromFileInfo = digestManager.verifyDigestAndReturnLac(explicitLacBufReadFromFileInfo);
            assertEquals("explicitLac persisted in FileInfo", (numOfEntries - 1), explicitLacReadFromFileInfo);
        } else {
            assertEquals("explicitLac is not expected to be persisted, so it should be null", null,
                    explicitLacBufReadFromFileInfo);
        }

        bkcWithExplicitLAC.close();
    }

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId Ledger Id
     *
     * @return file object.
     */
    private File getLedgerFile(long ledgerId, File[] indexDirectories) {
        String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
        File lf = null;
        for (File d : indexDirectories) {
            lf = new File(d, ledgerName);
            if (lf.exists()) {
                break;
            }
            lf = null;
        }
        return lf;
    }

    /**
     * Get FileInfo for a specified ledger.
     *
     * @param ledgerId Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId, File[] indexDirectories) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId, indexDirectories);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId
                    + ". It may be not flushed yet.");
        }
        ReadOnlyFileInfo fi = new ReadOnlyFileInfo(ledgerFile, null);
        fi.readHeader();
        return fi;
    }

    @Test
    public void testGetListOfEntriesOfLedger() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        int numOfBookies = bookieCount();
        int numOfEntries = 5;
        BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "testPasswd".getBytes());
        long lId = lh.getId();
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry("000".getBytes());
        }

        ServerConfiguration newBookieConf = new ServerConfiguration(confByIndex(0));
        /*
         * by reusing bookieServerConfig and setting metadataServiceUri to null
         * we can create/start new Bookie instance using the same data
         * (journal/ledger/index) of the existing BookeieServer for our testing
         * purpose.
         */
        newBookieConf.setMetadataServiceUri(null);
        BookieImpl newbookie = new TestBookieImpl(newBookieConf);
        /*
         * since 'newbookie' uses the same data as original Bookie, it should be
         * able to read journal of the original bookie.
         */
        newbookie.readJournal();

        OfLong listOfEntriesItr = newbookie.getListOfEntriesOfLedger(lId);
        ArrayList<Long> arrayList = new ArrayList<Long>();
        Consumer<Long> addMethod = arrayList::add;
        listOfEntriesItr.forEachRemaining(addMethod);

        assertEquals("Num Of Entries", numOfEntries, arrayList.size());
        Assert.assertTrue("Iterator should be sorted",
                IntStream.range(0, arrayList.size() - 1).allMatch(k -> arrayList.get(k) <= arrayList.get(k + 1)));
        bkc.close();
    }
}
