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
package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link TriggerAuditCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TriggerAuditCommand.class})
public class TriggerAuditCommandTest extends BookieCommandTestBase {

    private ClientConfiguration clientConfiguration;
    private BookKeeperAdmin admin;

    public TriggerAuditCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        clientConfiguration = mock(ClientConfiguration.class);
        PowerMockito.whenNew(ClientConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(clientConfiguration);

        admin = mock(BookKeeperAdmin.class);
        PowerMockito.whenNew(BookKeeperAdmin.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(admin);

        doNothing().when(admin).triggerAudit();
    }

    @Test
    public void testCommand() throws Exception {
        TriggerAuditCommand cmd = new TriggerAuditCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        verifyNew(ClientConfiguration.class, times(1)).withArguments(conf);
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(clientConfiguration);

        verify(admin, times(1)).triggerAudit();
    }
}
