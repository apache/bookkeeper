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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.bookie.storage.ldb.LocationsIndexRebuildOp;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link RebuildDBLedgerLocationsIndexCommand}.
 */
public class RebuildDBLedgerLocationsIndexCommandTest extends BookieCommandTestBase {

    public RebuildDBLedgerLocationsIndexCommandTest() {
        super(3, 0);
    }

    @Test
    public void testCommand() throws Exception {
        mockServerConfigurationConstruction();
        mockConstruction(LocationsIndexRebuildOp.class);

        RebuildDBLedgerLocationsIndexCommand command = new RebuildDBLedgerLocationsIndexCommand();
        Assert.assertTrue(command.apply(bkFlags, new String[] { "" }));

        verify(getMockedConstruction(LocationsIndexRebuildOp.class).constructed().get(0),
                times(1)).initiate();
    }
}
