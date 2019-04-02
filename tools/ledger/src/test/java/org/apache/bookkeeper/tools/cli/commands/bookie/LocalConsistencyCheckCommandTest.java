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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.LedgerStorage;
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
 * Unit test for {@link LocalConsistencyCheckCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ LocalConsistencyCheckCommand.class, Bookie.class })
public class LocalConsistencyCheckCommandTest extends BookieCommandTestBase {

    @Mock
    private ServerConfiguration serverConfiguration;

    @Mock
    private LedgerStorage ledgerStorage;

    public LocalConsistencyCheckCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ServerConfiguration.class).withArguments(eq(conf)).thenReturn(serverConfiguration);
        PowerMockito.mockStatic(Bookie.class);
        PowerMockito.when(Bookie.mountLedgerStorageOffline(eq(serverConfiguration), eq(null)))
                    .thenReturn(ledgerStorage);
        List<LedgerStorage.DetectedInconsistency> errors = new ArrayList<>();
        PowerMockito.when(ledgerStorage.localConsistencyCheck(eq(java.util.Optional.empty()))).thenReturn(errors);
    }

    @Test
    public void testCommand() throws Exception {
        LocalConsistencyCheckCommand cmd = new LocalConsistencyCheckCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] {}));
        verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        verify(ledgerStorage, times(1)).localConsistencyCheck(eq(java.util.Optional.empty()));
    }
}
