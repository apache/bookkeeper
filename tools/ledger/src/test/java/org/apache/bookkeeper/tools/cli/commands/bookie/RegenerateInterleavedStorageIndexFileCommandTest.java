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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.bookie.InterleavedStorageRegenerateIndexOp;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link RegenerateInterleavedStorageIndexFileCommand}.
 */
public class RegenerateInterleavedStorageIndexFileCommandTest extends BookieCommandTestBase {

    public RegenerateInterleavedStorageIndexFileCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        mockServerConfigurationConstruction();
    }

    @Test
    public void testCommand() throws Exception {
        String ledgerIds = "1,2,3";
        String password = "12345";

        mockConstruction(InterleavedStorageRegenerateIndexOp.class, (op, context) -> {
                         doNothing().when(op).initiate(anyBoolean());
        });
        RegenerateInterleavedStorageIndexFileCommand cmd = new RegenerateInterleavedStorageIndexFileCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-p", password, "-l", ledgerIds }));
        verify(getMockedConstruction(InterleavedStorageRegenerateIndexOp.class).constructed().get(0),
                times(1)).initiate(anyBoolean());
    }
}

