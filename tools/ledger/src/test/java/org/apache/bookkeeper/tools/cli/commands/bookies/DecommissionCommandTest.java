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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Unit test for {@link DecommissionCommand}.
 */
@SuppressWarnings("unchecked")
public class DecommissionCommandTest extends BookieCommandTestBase {

    private Versioned<Cookie> cookieVersioned;
    private static final String bookieID = UUID.randomUUID().toString();

    public DecommissionCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();


        mockServerConfigurationConstruction(conf -> {
            doReturn(bookieID).when(conf).getBookieId();
        });
        mockClientConfigurationConstruction();
        mockBookKeeperAdminConstruction();
        mockConstruction(BookieId.class, (mocked, context) -> {
            doReturn(bookieID).when(mocked).getId();
        });

        RegistrationManager registrationManager = mock(RegistrationManager.class);
        mockMetadataDriversWithRegistrationManager(registrationManager);


        cookieVersioned = mock(Versioned.class);
        final MockedStatic<Cookie> cookieMockedStatic = mockStatic(Cookie.class);
        cookieMockedStatic.when(() -> Cookie.readFromRegistrationManager(eq(registrationManager),
                        any(BookieId.class)))
                    .thenReturn(cookieVersioned);

        final Cookie cookie = mock(Cookie.class);
        final Version version = mock(Version.class);

        when(cookieVersioned.getValue()).thenReturn(cookie);
        when(cookieVersioned.getVersion()).thenReturn(version);
        doNothing().when(cookie)
                    .deleteFromRegistrationManager(eq(registrationManager), any(BookieId.class), eq(version));
    }

    @Test
    public void testWithoutBookieId() throws Exception {
        DecommissionCommand cmd = new DecommissionCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));

        verify(getMockedConstruction(BookKeeperAdmin.class).constructed().get(0),
                times(1)).decommissionBookie(any(BookieId.class));
        verify(cookieVersioned, times(1)).getValue();
        verify(cookieVersioned, times(1)).getVersion();
    }

    @Test
    public void testWithBookieId() throws Exception {
        DecommissionCommand cmd = new DecommissionCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-b", bookieID }));

        verify(getMockedConstruction(BookKeeperAdmin.class).constructed().get(0),
                times(1)).decommissionBookie(any(BookieId.class));
        verify(cookieVersioned, times(1)).getValue();
        verify(cookieVersioned, times(1)).getVersion();
    }
}
