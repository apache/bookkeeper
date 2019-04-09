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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.InterleavedStorageRegenerateIndexOp;
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
 * Unit test for {@link RegenerateInterleavedStorageIndexFileCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ RegenerateInterleavedStorageIndexFileCommand.class, InterleavedStorageRegenerateIndexOp.class,
    ServerConfiguration.class })
public class RegenerateInterleavedStorageIndexFileCommandTest extends BookieCommandTestBase {

    @Mock
    private ServerConfiguration serverConfiguration;

    @Mock
    private InterleavedStorageRegenerateIndexOp op;

    public RegenerateInterleavedStorageIndexFileCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ServerConfiguration.class).withArguments(eq(conf)).thenReturn(serverConfiguration);
    }

    @Test
    public void testCommand() throws Exception {
        String ledgerIds = "1,2,3";
        String password = "12345";
        Set<Long> ledgerIdsSet = Arrays.stream(ledgerIds.split(",")).map((id) -> Long.parseLong(id))
                                       .collect(Collectors.toSet());
        byte[] bytes = password.getBytes();
        PowerMockito.whenNew(InterleavedStorageRegenerateIndexOp.class)
                    .withArguments(eq(serverConfiguration), eq(ledgerIdsSet), eq(bytes)).thenReturn(op);
        PowerMockito.doNothing().when(op).initiate(anyBoolean());

        RegenerateInterleavedStorageIndexFileCommand cmd = new RegenerateInterleavedStorageIndexFileCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-p", password, "-l", ledgerIds }));
        PowerMockito.verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        PowerMockito.verifyNew(InterleavedStorageRegenerateIndexOp.class, times(1))
                    .withArguments(eq(serverConfiguration), eq(ledgerIdsSet), eq(bytes));
        verify(op, times(1)).initiate(anyBoolean());
    }
}

