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
package org.apache.bookkeeper.tools.cli.commands.bookies;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link InstanceIdCommand}.
 */
public class InstanceIdCommandTest extends BookieCommandTestBase {

    public InstanceIdCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        final RegistrationManager manager = mock(RegistrationManager.class);
        mockMetadataDriversWithRegistrationManager(manager);
        when(manager.getClusterInstanceId()).thenReturn("");
    }

    @Test
    public void testCommand() {
        InstanceIdCommand cmd = new InstanceIdCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] {}));
    }
}
