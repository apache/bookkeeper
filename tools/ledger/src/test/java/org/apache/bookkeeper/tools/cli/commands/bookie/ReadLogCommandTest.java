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
import static org.powermock.api.mockito.PowerMockito.doNothing;

import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.ReadOnlyEntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link ReadLogCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ReadLogCommand.class })
public class ReadLogCommandTest extends BookieCommandTestBase {

    @Mock
    private ReadOnlyEntryLogger entryLogger;

    @Mock
    private EntryLogger.EntryLogScanner entryLogScanner;

    public ReadLogCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ReadOnlyEntryLogger.class).withArguments(eq(conf)).thenReturn(entryLogger);
        doNothing().when(entryLogger).scanEntryLog(anyLong(), eq(entryLogScanner));
    }

    @Test
    public void testWithoutAnyFlags() {
        ReadLogCommand cmd = new ReadLogCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] {}));
    }

    @Test
    public void testWithEntryId() {
        ReadLogCommand cmd = new ReadLogCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-li", "1" }));
    }

    @Test
    public void testWithEntryFilename() {
        ReadLogCommand cmd = new ReadLogCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-f", "1.log" }));
    }

    @Test
    public void testWithErrorPos() {
        ReadLogCommand cmd = new ReadLogCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] { "-sp", "1", "-ep", "0", "-li", "1" }));
    }
}
