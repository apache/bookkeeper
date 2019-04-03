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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link NukeExistingClusterCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ NukeExistingClusterCommand.class, BookKeeperAdmin.class })
public class NukeExistingClusterCommandTest extends BookieCommandTestBase {

    public NukeExistingClusterCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

//        PowerMockito.mockStatic(BookKeeperAdmin.classs);
    }

    @Test
    public void commandWithoutFlags() throws Exception {
        NukeExistingClusterCommand cmd = new NukeExistingClusterCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] { "" }));
    }

    @Test
    public void commandWithoutForceAndInstanceId() {
        NukeExistingClusterCommand cmd = new NukeExistingClusterCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] { "-p", "" }));
    }

    @Test
    public void testCommand() throws Exception {
        PowerMockito.mockStatic(BookKeeperAdmin.class);
        PowerMockito.when(
            BookKeeperAdmin.nukeExistingCluster(any(ServerConfiguration.class), anyString(), anyString(), anyBoolean()))
                    .thenReturn(true);

        NukeExistingClusterCommand cmd = new NukeExistingClusterCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-p", "", "-i", "1" }));
    }
}
