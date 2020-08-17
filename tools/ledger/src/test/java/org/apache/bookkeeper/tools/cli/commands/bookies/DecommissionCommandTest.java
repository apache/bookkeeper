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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.UUID;
import java.util.function.Function;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link DecommissionCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DecommissionCommand.class, Bookie.class, MetadataDrivers.class, Cookie.class })
public class DecommissionCommandTest extends BookieCommandTestBase {

    @Mock
    private ClientConfiguration clientConfiguration;

    @Mock
    private BookKeeperAdmin bookKeeperAdmin;

    private BookieId bookieSocketAddress = BookieId.parse(UUID.randomUUID().toString());

    @Mock
    private Versioned<Cookie> cookieVersioned;

    @Mock
    private Cookie cookie;

    @Mock
    private Version version;
    public DecommissionCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ClientConfiguration.class).withArguments(eq(conf)).thenReturn(clientConfiguration);
        PowerMockito.whenNew(BookKeeperAdmin.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(bookKeeperAdmin);
        PowerMockito.mockStatic(Bookie.class);
        PowerMockito.when(Bookie.getBookieId(any(ServerConfiguration.class))).thenReturn(bookieSocketAddress);
        PowerMockito.doNothing().when(bookKeeperAdmin).decommissionBookie(eq(bookieSocketAddress));

        RegistrationManager registrationManager = mock(RegistrationManager.class);
        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, ?> f = invocationOnMock.getArgument(1);
            f.apply(registrationManager);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithRegistrationManager", any(ServerConfiguration.class),
                any(Function.class));

        PowerMockito.mockStatic(Cookie.class);
        PowerMockito.when(Cookie.readFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress)))
                    .thenReturn(cookieVersioned);
        when(cookieVersioned.getValue()).thenReturn(cookie);
        when(cookieVersioned.getVersion()).thenReturn(version);
        PowerMockito.doNothing().when(cookie)
                    .deleteFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress), eq(version));
    }

    @Test
    public void testWithoutBookieId() throws Exception {
        DecommissionCommand cmd = new DecommissionCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        verifyNew(ClientConfiguration.class, times(1)).withArguments(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1)).decommissionBookie(eq(bookieSocketAddress));
        verify(cookieVersioned, times(1)).getValue();
        verify(cookieVersioned, times(1)).getVersion();
    }

    @Test
    public void testWithBookieId() throws Exception {
        DecommissionCommand cmd = new DecommissionCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-b", bookieSocketAddress.getId() }));

        verifyNew(ClientConfiguration.class, times(1)).withArguments(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1)).decommissionBookie(eq(bookieSocketAddress));
        verify(cookieVersioned, times(1)).getValue();
        verify(cookieVersioned, times(1)).getVersion();
    }
}
