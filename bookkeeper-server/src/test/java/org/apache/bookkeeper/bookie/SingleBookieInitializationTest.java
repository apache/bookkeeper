/*
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
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.bookie.BookieException.Code;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test a single bookie at readonly mode.
 */
public class SingleBookieInitializationTest {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private File journalDir;
    private File ledgerDir;
    private ServerConfiguration conf;
    private Bookie bookie;

    @Before
    public void setUp() throws Exception {
        this.journalDir = testDir.newFolder("journal");
        this.ledgerDir = testDir.newFolder("ledgers");

        this.conf = TestBKConfiguration.newServerConfiguration();
        this.conf.setJournalDirsName(new String[] { journalDir.getAbsolutePath() });
        this.conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
        this.conf.setMetadataServiceUri(null);
    }

    @After
    public void tearDown() throws Exception {
        if (null != this.bookie) {
            this.bookie.shutdown();
        }
    }

    private static String generateDataString(long ledger, long entry) {
        return ("ledger-" + ledger + "-" + entry);
    }

    private static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    @Test
    public void testInitBookieNoWritableDirsButHasEnoughSpaces() throws Exception {
        float usage = 1.0f - ((float) ledgerDir.getUsableSpace()) / ledgerDir.getTotalSpace();
        conf.setDiskUsageThreshold(usage / 2);
        conf.setDiskUsageWarnThreshold(usage / 3);
        conf.setMinUsableSizeForEntryLogCreation(Long.MIN_VALUE);
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

        bookie = new TestBookieImpl(conf);
        bookie.start();

        CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
        bookie.addEntry(
            generateEntry(1L, 2L),
            false,
            (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc),
            null,
            new byte[0]
        );
        assertEquals(Code.OK, writeFuture.get().intValue());
    }

    @Test
    public void testInitBookieNoWritableDirsAndNoEnoughSpaces() throws Exception {
        float usage = 1.0f - ((float) ledgerDir.getUsableSpace()) / ledgerDir.getTotalSpace();
        conf.setDiskUsageThreshold(usage / 2);
        conf.setDiskUsageWarnThreshold(usage / 3);
        conf.setMinUsableSizeForEntryLogCreation(Long.MAX_VALUE);
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

        bookie = new TestBookieImpl(conf);
        bookie.start();

        try {
            bookie.addEntry(
                generateEntry(1L, 2L),
                false,
                (rc, ledgerId, entryId, addr, ctx) -> {},
                null,
                new byte[0]
            );
            fail("Should fail on creating new entry log file"
                + " since there is no enough disk space to accommodate writes");
        } catch (IOException ioe) {
            // expected
            assertTrue(ioe.getCause() instanceof NoWritableLedgerDirException);
        }
    }

}
