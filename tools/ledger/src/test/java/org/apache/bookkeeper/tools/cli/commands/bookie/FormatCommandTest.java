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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test {@link FormatCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FormatCommand.class, MetadataDrivers.class, Cookie.class })
public class FormatCommandTest extends BookieCommandTestBase {

    public FormatCommandTest() {
        super(3, 0);
    }

    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                .withArguments(eq(conf)).thenReturn(conf);

        PowerMockito.mockStatic(MetadataDrivers.class);
        RegistrationManager rm = mock(RegistrationManager.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, ?> func = invocationOnMock.getArgument(1);
            func.apply(rm);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithRegistrationManager", any(ServerConfiguration.class),
                any(Function.class));

        Versioned cookie = mock(Versioned.class);
        PowerMockito.whenNew(Versioned.class).withParameterTypes(Object.class, Version.class)
                .withArguments(any(Cookie.class), eq(new LongVersion(1L))).thenReturn(cookie);

        PowerMockito.mockStatic(Cookie.class);
        when(Cookie.readFromRegistrationManager(rm, conf)).thenReturn((Versioned<Cookie>) cookie);

        when(cookie.getValue()).thenReturn(mock(Cookie.class));
    }

    /**
     * Test different type of command flags.
     */
    @Test
    public void testNonInteraction() {
        testCommand("-n");
    }

    @Test
    public void testNonInteractionLongArgs() {
        testCommand("--noninteractive");
    }

    @Test
    public void testForce() {
        testCommand("-f");
    }

    @Test
    public void testForceLongArgs() {
        testCommand("--force");
    }

    @Test
    public void testDeleteCookie() {
        testCommand("-d");
    }

    @Test
    public void testDeleteCookieLongArgs() {
        testCommand("--deletecookie");
    }

    @Test
    public void testAllCommand() {
        testCommand("-n", "-f", "-d");
    }

    @Test
    public void testAllCommandLongArgs() {
        testCommand("--noninteractive", "--force", "--deletecookie");
    }

    private void testCommand(String... args) {
        FormatCommand cmd = new FormatCommand();
        try {
            assertTrue(cmd.apply(bkFlags, args));
        } catch (Exception e) {
            fail("Should not throw any exception here");
        }
    }

}
