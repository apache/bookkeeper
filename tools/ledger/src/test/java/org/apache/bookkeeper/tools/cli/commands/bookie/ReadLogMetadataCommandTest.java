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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;

import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.ReadOnlyEntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link ReadLogMetadataCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ReadLogMetadataCommand.class })
public class ReadLogMetadataCommandTest extends BookieCommandTestBase {

    @Mock
    private ReadOnlyEntryLogger entryLogger;

    @Mock
    private EntryLogMetadata entryLogMetadata;

    public ReadLogMetadataCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        PowerMockito.whenNew(ReadOnlyEntryLogger.class).withArguments(eq(conf)).thenReturn(entryLogger);
        when(entryLogger.getEntryLogMetadata(anyLong())).thenReturn(entryLogMetadata);

        ConcurrentLongLongHashMap map = new ConcurrentLongLongHashMap();
        map.put(1, 1);
        when(entryLogMetadata.getLedgersMap()).thenReturn(map);

    }

    @Test
    public void testWithoutFlags() {
        ReadLogMetadataCommand cmd = new ReadLogMetadataCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] {"-l", "-1", "-f", ""}));
    }

    @Test
    public void commandTest() throws Exception {
        ReadLogMetadataCommand cmd = new ReadLogMetadataCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1" }));
        verifyNew(ReadOnlyEntryLogger.class, times(1)).withArguments(eq(conf));
        verify(entryLogger, times(1)).getEntryLogMetadata(anyLong());
        verify(entryLogMetadata, times(1)).getLedgersMap();
    }
}

