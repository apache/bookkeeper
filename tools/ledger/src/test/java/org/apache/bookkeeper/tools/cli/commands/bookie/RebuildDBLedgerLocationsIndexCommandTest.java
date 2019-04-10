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

import org.apache.bookkeeper.bookie.storage.ldb.LocationsIndexRebuildOp;
import org.apache.bookkeeper.conf.AbstractConfiguration;
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
 * Unit test for {@link RebuildDBLedgerLocationsIndexCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ RebuildDBLedgerLocationsIndexCommand.class })
public class RebuildDBLedgerLocationsIndexCommandTest extends BookieCommandTestBase {

    @Mock
    private LocationsIndexRebuildOp locationsIndexRebuildOp;

    public RebuildDBLedgerLocationsIndexCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(conf);

        PowerMockito.whenNew(LocationsIndexRebuildOp.class).withParameterTypes(ServerConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(locationsIndexRebuildOp);
        PowerMockito.doNothing().when(locationsIndexRebuildOp).initiate();
    }

    @Test
    public void testCommand() throws Exception {
        RebuildDBLedgerLocationsIndexCommand command = new RebuildDBLedgerLocationsIndexCommand();
        Assert.assertTrue(command.apply(bkFlags, new String[] { "" }));

        PowerMockito.verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        PowerMockito.verifyNew(LocationsIndexRebuildOp.class, times(1)).withArguments(eq(conf));
        verify(locationsIndexRebuildOp, times(1)).initiate();
    }
}
