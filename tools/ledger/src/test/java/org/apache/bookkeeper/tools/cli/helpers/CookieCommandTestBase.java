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

package org.apache.bookkeeper.tools.cli.helpers;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.util.function.Function;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * A test base for testing cookie commands.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ MetadataDrivers.class })
public class CookieCommandTestBase extends CommandTestBase {

    protected static final String INVALID_BOOKIE_ID = "127.0.0.1";
    protected static final String BOOKIE_ID = "127.0.0.1:3181";

    protected RegistrationManager rm;

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(MetadataDrivers.class);
        this.rm = mock(RegistrationManager.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, ?> func = invocationOnMock.getArgument(1);
            func.apply(rm);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithRegistrationManager",
            any(ServerConfiguration.class), any(Function.class));
    }

    protected void assertBookieIdMissing(String consoleOutput) {
        assertTrue(
            consoleOutput,
            consoleOutput.contains("No bookie id or more bookie ids is specified")
        );
    }

    protected void assertInvalidBookieId(String consoleOutput, String bookieId) {
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Invalid bookie id '" + bookieId + "'"));
    }

    protected void assertOptionMissing(String consoleOutput, String option) {
        assertTrue(
            consoleOutput,
            consoleOutput.contains("The following option is required: " + option));
    }

    protected void assertPrintUsage(String consoleOutput, String usage) {
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Usage:  " + usage));
    }

    protected void assertCookieFileNotExists(String consoleOutput, String cookieFile) {
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Cookie file '" + cookieFile + "' doesn't exist."));
    }

}
