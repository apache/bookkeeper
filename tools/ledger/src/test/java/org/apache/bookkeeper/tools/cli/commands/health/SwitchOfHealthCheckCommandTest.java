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
package org.apache.bookkeeper.tools.cli.commands.health;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link SwitchOfHealthCheckCommand}.
 */
public class SwitchOfHealthCheckCommandTest extends BookieCommandTestBase {
    MetadataClientDriver metadataClientDriver;
    private MetadataBookieDriver metadataBookieDriver;


    public SwitchOfHealthCheckCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        mockServerConfigurationConstruction();
        metadataBookieDriver = mock(MetadataBookieDriver.class);
        mockMetadataDriversWithMetadataBookieDriver(metadataBookieDriver);
        metadataClientDriver = mock(MetadataClientDriver.class);
        when(metadataBookieDriver.isHealthCheckEnabled()).thenReturn(FutureUtils.value(true));
        when(metadataBookieDriver.disableHealthCheck()).thenReturn(FutureUtils.value(null));
    }

    @Test
    public void testWithEnable() {
        testCommand("-e");
    }

    @Test
    public void testWithStatus() {
        testCommand("-s");
    }


    @Test
    public void testWithNoArgsDisable() {
        testCommand("");
    }

    private void testCommand(String... args) {
        SwitchOfHealthCheckCommand cmd = new SwitchOfHealthCheckCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
