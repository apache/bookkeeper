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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.zookeeper.AsyncCallback;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for ListLedgers command.
 */
public class ListLedgersCommandTest extends BookieCommandTestBase {

    private static final BookieId bookieAddress = BookieId.parse(UUID.randomUUID().toString());

    public ListLedgersCommandTest() {
        super(3, 3);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setup() throws Exception {
        super.setup();

        mockServerConfigurationConstruction();

        mockConstruction(BookieId.class, (bookieId, context) -> {
            doReturn(bookieAddress.getId()).when(bookieId).getId();
        });

        LedgerManagerFactory mFactory = mock(LedgerManagerFactory.class);
        mockMetadataDriversWithLedgerManagerFactory(mFactory);

        LedgerManager ledgerManager = mock(LedgerManager.class);
        when(mFactory.newLedgerManager()).thenReturn(ledgerManager);

        mockConstruction(CountDownLatch.class);

        AsyncCallback.VoidCallback callback = mock(AsyncCallback.VoidCallback.class);
        doAnswer(invocationOnMock -> {
            getMockedConstruction(CountDownLatch.class).constructed().get(0).countDown();
            return null;
        }).when(callback).processResult(anyInt(), anyString(), any());
    }

    @Test
    public void testWithoutBookieId() {
        testCommand("");
    }

    @Test
    public void testWithBookieId() {
        testCommand("-id", bookieAddress.getId());
    }

    private void testCommand(String... args) {
        ListLedgersCommand command = new ListLedgersCommand();
        Assert.assertTrue(command.apply(bkFlags, args));
    }
}
