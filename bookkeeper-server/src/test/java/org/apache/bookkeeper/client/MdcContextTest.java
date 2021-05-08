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

package org.apache.bookkeeper.client;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Test passing of MDC context.
 */
@SuppressWarnings("deprecation")
@Slf4j
public class MdcContextTest extends BookKeeperClusterTestCase {
    public static final String MDC_REQUEST_ID = "request_id";

    final byte[] entry = "Test Entry".getBytes();

    BookKeeper bkc;
    LedgerHandle lh;

    private Appender mockAppender;
    private Queue<String> capturedEvents;
    private Logger rootLogger = LogManager.getRootLogger();

    public MdcContextTest() {
        super(3);
        baseConf.setNumAddWorkerThreads(0);
        baseConf.setNumReadWorkerThreads(0);
        baseConf.setPreserveMdcForTaskExecution(true);
        baseConf.setReadOnlyModeEnabled(true);

        // for read-only bookie
        baseConf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE);
    }


    public static String mdcFormat(Object mdc, String message) {
        return mdc == null
                ? "[" + MDC_REQUEST_ID + ":] - " + message
                : "[" + MDC_REQUEST_ID + ":" + mdc.toString()
                + "] - " + message;
    }

    public void assertLogWithMdc(String mdc, String msgSubstring) {
        assertThat(capturedEvents,
                    hasItem(CoreMatchers.allOf(
                        containsString("[" + MDC_REQUEST_ID + ":" + mdc + "] - "),
                        containsString(msgSubstring)
                    )));
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setReadTimeout(360)
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setPreserveMdcForTaskExecution(true);

        MDC.clear();
        bkc = new BookKeeper(conf);

        MDC.put(MDC_REQUEST_ID, "ledger_create");
        log.info("creating ledger");
        lh = bkc.createLedgerAdv(3, 3, 3, BookKeeper.DigestType.CRC32, new byte[] {});
        MDC.clear();

        mockAppender = mock(Appender.class);
        when(mockAppender.getName()).thenReturn("MockAppender");

        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.INFO);
        capturedEvents = new ConcurrentLinkedQueue<>();

        doAnswer(answerVoid((LoggingEvent event) -> capturedEvents.add(
                    mdcFormat(event.getMDC(MDC_REQUEST_ID), event.getRenderedMessage())
            ))).when(mockAppender).doAppend(any());
    }

    @After
    public void tearDown() throws Exception {
        lh.close();
        bkc.close();
        rootLogger.removeAppender(mockAppender);
        capturedEvents = null;
        MDC.clear();
        super.tearDown();
    }

    @Test
    public void testLedgerCreateFails() throws Exception {
        MDC.put(MDC_REQUEST_ID, "ledger_create_fail");
        try {
            bkc.createLedgerAdv(99, 3, 2, BookKeeper.DigestType.CRC32, new byte[]{});
            Assert.fail("should not get here");
        } catch (BKException bke) {
            // expected
        }
        assertLogWithMdc("ledger_create_fail", "Not enough bookies to create ledger");
    }

    @Test
    public void testSimpleAdd() throws Exception {
        MDC.put(MDC_REQUEST_ID, "ledger_add_entry");
        lh.addEntry(0, entry);

        // client msg
        assertLogWithMdc("ledger_add_entry", "Successfully connected to bookie");
        // bookie msg
        assertLogWithMdc("ledger_add_entry", "Created new entry log file");
    }

    @Test
    public void testAddWithEnsembleChange() throws Exception {
        lh.addEntry(0, entry);
        startNewBookie();
        killBookie(0);

        MDC.put(MDC_REQUEST_ID, "ledger_add_entry");
        lh.addEntry(1, entry);
        assertLogWithMdc("ledger_add_entry", "Could not connect to bookie");
        assertLogWithMdc("ledger_add_entry", "Failed to write entry");
        //commented out until we figure out a way to preserve MDC through a call out
        //to another thread pool
        //assertLogWithMdc("ledger_add_entry", "New Ensemble");
    }

    @Test
    public void testAddFailsWithReadOnlyBookie() throws Exception {
        for (int i = 0; i < 3; ++i) {
            Bookie bookie = bs.get(i).getBookie();
            File[] ledgerDirs = bsConfs.get(i).getLedgerDirs();
            LedgerDirsManager ledgerDirsManager = bookie.getLedgerDirsManager();
            ledgerDirsManager.addToFilledDirs(new File(ledgerDirs[0], "current"));
        }

        MDC.put(MDC_REQUEST_ID, "ledger_add_entry");
        try {
            lh.addEntry(0, entry);
            Assert.fail("should not get here");
        } catch (BKException bke) {
            // expected, pass
        }

        assertLogWithMdc("ledger_add_entry", "No writable ledger dirs below diskUsageThreshold");
        assertLogWithMdc("ledger_add_entry", "All ledger directories are non writable and no reserved space");
        assertLogWithMdc("ledger_add_entry", "Error writing entry:0 to ledger:0");
        assertLogWithMdc("ledger_add_entry", "Add for failed on bookie");
        assertLogWithMdc("ledger_add_entry", "Failed to find 1 bookies");
        assertLogWithMdc("ledger_add_entry", "Closing ledger 0 due to NotEnoughBookiesException");
    }

    @Test
    public void testAddFailsDuplicateEntry() throws Exception {
        lh.addEntry(0, entry);

        MDC.put(MDC_REQUEST_ID, "ledger_add_duplicate_entry");
        try {
            lh.addEntry(0, entry);
            Assert.fail("should not get here");
        } catch (BKException bke) {
            // expected, pass
        }

        assertLogWithMdc("ledger_add_duplicate_entry", "Trying to re-add duplicate entryid:0");
        assertLogWithMdc("ledger_add_duplicate_entry", "Write of ledger entry to quorum failed");
    }

    @Test
    public void testReadEntryBeyondLac() throws Exception {
        MDC.put(MDC_REQUEST_ID, "ledger_read_entry");

        try {
            lh.readEntries(100, 100);
            fail("should not get here");
        } catch (BKException.BKReadException e) {
            // pass
        }
        assertLogWithMdc("ledger_read_entry", "ReadEntries exception on ledgerId:0 firstEntry:100 lastEntry:100");
    }

    @Test
    public void testReadFromDeletedLedger() throws Exception {
        lh.addEntry(0, entry);
        lh.close();
        bkc.deleteLedger(lh.ledgerId);

        MDC.put(MDC_REQUEST_ID, "ledger_read_entry");

        try {
            lh.readEntries(100, 100);
            fail("should not get here");
        } catch (BKException.BKReadException e) {
            // pass
        }
        assertLogWithMdc("ledger_read_entry", "ReadEntries exception on ledgerId:0 firstEntry:100 lastEntry:100");
    }

}
