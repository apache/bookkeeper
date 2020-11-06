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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link LedgerMetaDataCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ LedgerMetaDataCommand.class, MetadataDrivers.class, LedgerMetadata.class,
    LedgerManagerFactory.class })
public class LedgerMetaDataCommandTest extends BookieCommandTestBase {

    private LedgerManager ledgerManager;
    private LedgerManagerFactory factory;
    private LedgerMetadataSerDe serDe;

    @Mock
    private CompletableFuture<Versioned<LedgerMetadata>> future;

    private TemporaryFolder folder = new TemporaryFolder();

    public LedgerMetaDataCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        folder.create();

        factory = mock(LedgerManagerFactory.class);

        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<LedgerManagerFactory, ?> function = invocationOnMock.getArgument(1);
            function.apply(factory);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithLedgerManagerFactory", any(ServerConfiguration.class),
                any(Function.class));

        ledgerManager = mock(LedgerManager.class);
        when(factory.newLedgerManager()).thenReturn(ledgerManager);

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getMetadataFormatVersion()).thenReturn(1);
        Versioned<LedgerMetadata> versioned = new Versioned<LedgerMetadata>(ledgerMetadata, Version.NEW);
        versioned.setValue(ledgerMetadata);
        when(future.join()).thenReturn(versioned);
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(future);
        when(ledgerManager.readLedgerMetadata(anyLong())).thenReturn(future);
        when(future.get()).thenReturn(versioned);

        serDe = mock(LedgerMetadataSerDe.class);
        whenNew(LedgerMetadataSerDe.class).withNoArguments().thenReturn(serDe);
        when(serDe.serialize(eq(ledgerMetadata))).thenReturn(new byte[0]);
        when(serDe.parseConfig(eq(new byte[0]), anyLong(), eq(Optional.empty()))).thenReturn(ledgerMetadata);
        when(ledgerManager.createLedgerMetadata(anyLong(), eq(ledgerMetadata))).thenReturn(future);
    }

    @Test
    public void testWithDumpToFile() throws IOException {
        File file = folder.newFile("testdump");
        LedgerMetaDataCommand cmd = new LedgerMetaDataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-d", file.getAbsolutePath() }));

        verify(ledgerManager, times(1)).readLedgerMetadata(anyLong());
        verify(serDe, times(1)).serialize(any(LedgerMetadata.class));
    }

    @Test
    public void testWithRestoreFromFile() throws IOException {
        File file = folder.newFile("testrestore");
        LedgerMetaDataCommand cmd = new LedgerMetaDataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-r", file.getAbsolutePath() }));

        verify(serDe, times(1)).parseConfig(eq(new byte[0]), anyLong(), eq(Optional.empty()));
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
