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
 * Unit test for {@link MetaFormatCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ MetaFormatCommand.class, BookKeeperAdmin.class })
public class MetaFormatCommandTest extends BookieCommandTestBase {

    public MetaFormatCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.mockStatic(BookKeeperAdmin.class);
        PowerMockito.when(BookKeeperAdmin.class, "format", any(ServerConfiguration.class), anyBoolean(), anyBoolean())
                    .thenReturn(true);

    }

    @Test
    public void testCommand() {
        MetaFormatCommand cmd = new MetaFormatCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
    }
}
