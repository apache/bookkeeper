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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.URI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link WhoIsAuditorCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ WhoIsAuditorCommand.class, ZKMetadataDriverBase.class, ZooKeeperClient.class, AuditorElector.class,
    CommandHelpers.class
})
public class WhoIsAuditorCommandTest extends BookieCommandTestBase {

    public WhoIsAuditorCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        PowerMockito.mockStatic(ZKMetadataDriverBase.class);
        PowerMockito.when(ZKMetadataDriverBase.getZKServersFromServiceUri(eq(URI.create(conf.getMetadataServiceUri()))))
                    .thenReturn("");

        ZooKeeperClient zk = mock(ZooKeeperClient.class);
        ZooKeeperClient.Builder builder = mock(ZooKeeperClient.Builder.class);
        PowerMockito.mockStatic(ZooKeeperClient.class);
        PowerMockito.when(ZooKeeperClient.newBuilder()).thenReturn(builder);
        when(builder.connectString(anyString())).thenReturn(builder);
        when(builder.sessionTimeoutMs(anyInt())).thenReturn(builder);
        when(builder.build()).thenReturn(zk);

        BookieSocketAddress bookieId = mock(BookieSocketAddress.class);

        PowerMockito.mockStatic(AuditorElector.class);
        PowerMockito.when(AuditorElector.getCurrentAuditor(eq(conf), eq(zk)))
                    .thenReturn(bookieId);

        PowerMockito.mockStatic(CommandHelpers.class);
        PowerMockito.when(CommandHelpers.getBookieSocketAddrStringRepresentation(eq(bookieId))).thenReturn("");
    }

    @Test
    public void testCommand() {
        WhoIsAuditorCommand cmd = new WhoIsAuditorCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
    }
}
