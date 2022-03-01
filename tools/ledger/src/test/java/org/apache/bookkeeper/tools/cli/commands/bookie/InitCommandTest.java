/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Test;

/**
 * Unit test for {@link InitCommand}.
 */
public class InitCommandTest extends BookieCommandTestBase {

    public InitCommandTest() {
        super(3, 0);
    }

    public void setup() throws Exception {
        mockServerConfigurationConstruction();
        mockBookKeeperAdminConstruction();
        mockStatic(BookKeeperAdmin.class).when(() -> BookKeeperAdmin.initBookie(any(ServerConfiguration.class)))
                .thenReturn(true);
    }

    @Test
    public void testInitCommand() {
            InitCommand initCommand = new InitCommand();
            try {
                initCommand.apply(bkFlags, new String[]{""});
            } catch (Exception e) {
                fail("Should not throw any exception here.");
            }

    }
}
