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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;

import org.apache.bookkeeper.bookie.ReadOnlyDefaultEntryLogger;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ReadLogCommand}.
 */
public class ReadLogCommandTest extends BookieCommandTestBase {

    public ReadLogCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockServerConfigurationConstruction();
        mockConstruction(ReadOnlyDefaultEntryLogger.class, (entryLogger, context) -> {
            doNothing().when(entryLogger).scanEntryLog(anyLong(), any(EntryLogScanner.class));
        });
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
