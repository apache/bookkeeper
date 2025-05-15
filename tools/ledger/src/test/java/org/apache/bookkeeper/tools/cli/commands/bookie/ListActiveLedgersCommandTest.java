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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.ReadOnlyDefaultEntryLogger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ListActiveLedgersCommand}.
 */
public class ListActiveLedgersCommandTest extends BookieCommandTestBase {
    private EntryLogMetadata entryLogMetadata;

    public ListActiveLedgersCommandTest() {
        super(3, 3);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockServerConfigurationConstruction();

        LedgerManagerFactory mFactory = mock(LedgerManagerFactory.class);
        mockMetadataDriversWithLedgerManagerFactory(mFactory);

        mockStatic(LedgerIdFormatter.class)
                .when(() -> LedgerIdFormatter.newLedgerIdFormatter(any()))
                .thenReturn(new LedgerIdFormatter.LongLedgerIdFormatter());

        LedgerManager ledgerManager = mock(LedgerManager.class);
        when(mFactory.newLedgerManager()).thenReturn(ledgerManager);

        doAnswer(invocation -> {
            BookkeeperInternalCallbacks.Processor<Long> processor = invocation.getArgument(0);
            AsyncCallback.VoidCallback cb = mock(AsyncCallback.VoidCallback.class);
            processor.process(101L, cb); // only legerId-101 on metadata

            AsyncCallback.VoidCallback callback = invocation.getArgument(1);
            callback.processResult(BKException.Code.OK, "", null);
            return true;
        }).when(ledgerManager).asyncProcessLedgers(any(BookkeeperInternalCallbacks.Processor.class), any(AsyncCallback.VoidCallback.class),
                any(), anyInt(), anyInt());

        entryLogMetadata = createEntryLogMeta();
        mockConstruction(ReadOnlyDefaultEntryLogger.class, (entryLogger, context) ->  {
            when(entryLogger.getEntryLogMetadata(anyLong())).thenReturn(entryLogMetadata);
        });
    }

    @Test
    public void testCommand() {
        ListActiveLedgersCommand command = new ListActiveLedgersCommand();;
        Assert.assertTrue(command.apply(bkFlags, new String[] {"-l", "0", "-t", "1000000"}));

        EntryLogMetadata entryLogMetadataToPrint = createEntryLogMeta();
        entryLogMetadataToPrint.removeLedgerIf(lId -> lId == 100L);

        Assert.assertEquals(entryLogMetadataToPrint.getTotalSize(), entryLogMetadata.getTotalSize());
        Assert.assertEquals(entryLogMetadataToPrint.getRemainingSize(), entryLogMetadata.getRemainingSize());
        Assert.assertEquals(entryLogMetadataToPrint.getUsage(), entryLogMetadata.getUsage(), 0.0);
    }

    private EntryLogMetadata createEntryLogMeta() {
        EntryLogMetadata entryLogMetadata = new EntryLogMetadata(0);
        entryLogMetadata.addLedgerSize(100, 10);
        entryLogMetadata.addLedgerSize(101, 20);
        return entryLogMetadata;
    }
}
