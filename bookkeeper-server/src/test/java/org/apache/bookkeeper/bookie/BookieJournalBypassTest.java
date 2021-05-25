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
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Tests that we're skipping journal when it's configured to do so.
 */
@Slf4j
public class BookieJournalBypassTest extends BookKeeperClusterTestCase {

    private int bookieIdx = 0;

    public BookieJournalBypassTest() {
        super(2);
    }

    @Override
    protected BookieServer startBookie(ServerConfiguration conf) throws Exception {
        if (bookieIdx++ == 0) {
            // First bookie will have the journal disabled
            conf.setJournalWriteData(false);
        }
        return super.startBookie(conf);
    }

    @Test
    public void testJournalBypass() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);

        BookieImpl bookieImpl = (BookieImpl) bs.get(0).getBookie();
        Journal journal0 = bookieImpl.journals.get(0);
        LedgerStorage ls0 = bs.get(0).getBookie().getLedgerStorage();

        bookieImpl = (BookieImpl) bs.get(1).getBookie();
        Journal journal1 = bookieImpl.journals.get(0);
        LedgerStorage ls1 = bs.get(1).getBookie().getLedgerStorage();

        ls0.flush();
        ls1.flush();

        long bk0OffsetBefore = journal0.getLastLogMark().getCurMark().getLogFileOffset();
        long bk1OffsetBefore = journal1.getLastLogMark().getCurMark().getLogFileOffset();

        writeEntries(conf);
        ls0.flush();
        ls1.flush();

        long bk0OffsetAfter = journal0.getLastLogMark().getCurMark().getLogFileOffset();
        long bk1OffsetAfter = journal1.getLastLogMark().getCurMark().getLogFileOffset();

        int flushDelta = 10 * 1024;
        int dataSize = 10 * 1024 * 1024;

        // Offset for journal-0 will be very close to previous point, just few KBs when flushing
        assertEquals(bk0OffsetBefore, bk0OffsetAfter, flushDelta);

        // Offset for journal-0 should have changed with the data size
        assertEquals(bk1OffsetBefore + dataSize, bk1OffsetAfter, flushDelta);
    }

    private void writeEntries(ClientConfiguration conf)
            throws Exception {
        @Cleanup
        BookKeeper bkc = new BookKeeper(conf);

        @Cleanup
        WriteHandle wh = bkc.newCreateLedgerOp()
                .withEnsembleSize(2)
                .withWriteQuorumSize(2)
                .withAckQuorumSize(2)
                .withPassword("".getBytes())
                .execute()
                .join();

        for (int i = 0; i < 10; i++) {
            wh.append(new byte[1024 * 1024]);
        }
    }
}
