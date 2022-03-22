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
package org.apache.bookkeeper.tools.cli.commands.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link LedgerMetaDataCommand}.
 */
@SuppressWarnings("unchecked")
public class LedgerMetaDataCommandTest extends BookieCommandTestBase {

    private LedgerManager ledgerManager;
    private LedgerManagerFactory factory;
    private CompletableFuture<Versioned<LedgerMetadata>> future;


    public LedgerMetaDataCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        factory = mock(LedgerManagerFactory.class);

        mockMetadataDriversWithLedgerManagerFactory(factory);
        ledgerManager = mock(LedgerManager.class);
        when(factory.newLedgerManager()).thenReturn(ledgerManager);

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getMetadataFormatVersion()).thenReturn(1);
        Versioned<LedgerMetadata> versioned = new Versioned<LedgerMetadata>(ledgerMetadata, Version.NEW);
        versioned.setValue(ledgerMetadata);
        CompletableFuture<Versioned<LedgerMetadata>> future = mock(CompletableFuture.class);
        when(future.join()).thenReturn(versioned);
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(future);
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(future);
        when(future.get()).thenReturn(versioned);

        mockConstruction(LedgerMetadataSerDe.class, (serDe, context) -> {
            when(serDe.serialize(eq(ledgerMetadata))).thenReturn(new byte[0]);
            when(serDe.parseConfig(eq(new byte[0]), anyLong(), eq(Optional.empty()))).thenReturn(ledgerMetadata);
        });

        when(ledgerManager.createLedgerMetadata(anyLong(), eq(ledgerMetadata))).thenReturn(future);
    }

    @Test
    public void testWithDumpToFile() throws IOException {
        File file = testDir.newFile("testdump");
        LedgerMetaDataCommand cmd = new LedgerMetaDataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-d", file.getAbsolutePath() }));

        verify(ledgerManager, times(1)).readLedgerMetadata(anyLong());
        verify(getMockedConstruction(LedgerMetadataSerDe.class).constructed().get(0),
                times(1)).serialize(any(LedgerMetadata.class));
    }

    @Test
    public void testWithRestoreFromFile() throws IOException {
        File file = testDir.newFile("testrestore");
        LedgerMetaDataCommand cmd = new LedgerMetaDataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-r", file.getAbsolutePath() }));

        verify(getMockedConstruction(LedgerMetadataSerDe.class).constructed().get(0),
                times(1)).parseConfig(eq(new byte[0]), anyLong(), eq(Optional.empty()));
        verify(ledgerManager, times(1)).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
    }

    @Test
    public void testWithoutArgs() {
        LedgerMetaDataCommand cmd = new LedgerMetaDataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1" }));

        verify(factory, times(1)).newLedgerManager();
        verify(ledgerManager, times(1)).readLedgerMetadata(anyLong());
    }

}
