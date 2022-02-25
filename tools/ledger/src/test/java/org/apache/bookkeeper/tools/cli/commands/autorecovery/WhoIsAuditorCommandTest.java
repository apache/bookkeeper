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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.UUID;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link WhoIsAuditorCommand}.
 */
public class WhoIsAuditorCommandTest extends BookieCommandTestBase {

    public WhoIsAuditorCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockClientConfigurationConstruction();

        ZooKeeperClient zk = mock(ZooKeeperClient.class);
        ZooKeeperClient.Builder builder = mock(ZooKeeperClient.Builder.class);

        mockStatic(ZooKeeperClient.class).when(() -> ZooKeeperClient.newBuilder()).thenReturn(builder);
        when(builder.connectString(anyString())).thenReturn(builder);
        when(builder.sessionTimeoutMs(anyInt())).thenReturn(builder);
        when(builder.build()).thenReturn(zk);

        BookieId bookieId = BookieId.parse(UUID.randomUUID().toString());

        mockStatic(CommandHelpers.class, CALLS_REAL_METHODS).when(() -> CommandHelpers
                .getBookieSocketAddrStringRepresentation(
                        eq(bookieId), any(BookieAddressResolver.class))).thenReturn("");
    }

    @Test
    public void testCommand() throws Exception {
        @Cleanup
        BookKeeperAdmin bka = mock(BookKeeperAdmin.class);
        when(bka.getCurrentAuditor()).thenReturn(BookieId.parse("127.0.0.1:3181"));
        WhoIsAuditorCommand cmd = new WhoIsAuditorCommand(bka);
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
    }
}
