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
package org.apache.bookkeeper.tools.cli.commands.cookie;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for {@link AdminCommand}.
 */
@SuppressWarnings("unchecked")
public class AdminCommandTest extends BookieCommandTestBase {

    private static final BookieId bookieSocketAddress = BookieId.parse("localhost:9000");

    private Versioned<Cookie> cookieVersioned;
    private Cookie cookie;

    public AdminCommandTest() throws IOException {
        super(3, 3);
    }

    @Override
    protected void mockServerConfigurationConstruction(Consumer<ServerConfiguration> consumer) {
        Consumer<ServerConfiguration> compositeConsumer = (serverConfiguration) -> {
            doReturn(bookieSocketAddress.getId()).when(serverConfiguration).getBookieId();
            if (consumer != null) {
                consumer.accept(serverConfiguration);
            }
        };
        super.mockServerConfigurationConstruction(compositeConsumer);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        cookieVersioned = mock(Versioned.class);
        cookie = mock(Cookie.class);

        mockStatic(Cookie.class);
        mockStatic(BookieImpl.class);

        mockUpdateBookieIdInCookie();
        mockVerifyCookie();
        mockInitDirecotory();
        mockExpandStorage();
        mockListOrDeleteCookies();

    }

    private void mockInitDirecotory() throws IOException {
        File[] files = new File[1];
        files[0] = testDir.getRoot();
        testDir.newFile(BookKeeperConstants.VERSION_FILENAME);
        getMockedStatic(BookieImpl.class).when(() -> BookieImpl.getCurrentDirectories(any())).thenReturn(files);
    }

    private void mockUpdateBookieIdInCookie() throws Exception {
        RegistrationManager registrationManager = mock(RegistrationManager.class);

        mockMetadataDriversWithRegistrationManager(registrationManager);

        getMockedStatic(Cookie.class).when(() -> Cookie
                .readFromRegistrationManager(eq(registrationManager), any(ServerConfiguration.class)))
                        .thenReturn(cookieVersioned);

        getMockedStatic(Cookie.class).when(() -> Cookie
                        .readFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress)))
                .thenReturn(cookieVersioned);

        when(cookieVersioned.getValue()).thenReturn(cookie);
        Cookie.Builder builder = mock(Cookie.Builder.class);
        getMockedStatic(Cookie.class).when(() -> Cookie.newBuilder(eq(cookie))).thenReturn(builder);
        when(builder.setBookieId(anyString())).thenReturn(builder);
        when(builder.build()).thenReturn(cookie);

        Version version = mock(Version.class);
        when(cookieVersioned.getVersion()).thenReturn(version);
        when(cookieVersioned.getValue()).thenReturn(cookie);
        doNothing().when(cookie)
                   .deleteFromRegistrationManager(
                           eq(registrationManager),  any(ServerConfiguration.class), eq(version));

        doNothing().when(cookie).writeToDirectory(any(File.class));
        doNothing().when(cookie)
                   .writeToRegistrationManager(
                           eq(registrationManager),  any(ServerConfiguration.class), eq(Version.NEW));

        doNothing().when(cookie)
                   .deleteFromRegistrationManager(
                           eq(registrationManager), any(ServerConfiguration.class), eq(version));
    }

    private void mockVerifyCookie() throws IOException, BookieException.InvalidCookieException {
        getMockedStatic(Cookie.class).when(() -> Cookie
                        .readFromDirectory(any(File.class)))
                .thenReturn(cookie);
        doNothing().when(cookie).verify(any(Cookie.class));
    }

    private void mockExpandStorage() throws Exception {
        MetadataBookieDriver metadataBookieDriver = mock(MetadataBookieDriver.class);
        RegistrationManager registrationManager = mock(RegistrationManager.class);
        mockMetadataDriversWithMetadataBookieDriver(metadataBookieDriver);
        when(metadataBookieDriver.createRegistrationManager()).thenReturn(registrationManager);
    }

    private void mockListOrDeleteCookies() throws UnknownHostException {
        getMockedStatic(BookieImpl.class).when(() -> BookieImpl.getBookieId(any(ServerConfiguration.class)))
                .thenReturn(bookieSocketAddress);
    }

    @Test
    public void testWithoutAnyFlags() {
        AdminCommand cmd = new AdminCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] {""}));
    }

    @Test
    public void testWithHostName() throws Exception {
        mockServerConfigurationConstruction(serverConfiguration -> {
            doReturn(true).when(serverConfiguration).getUseHostNameAsBookieID();
        });
        testCommand("-host");
        verify(cookie, times(2)).verify(any(Cookie.class));
        verify(getMockedConstruction(ServerConfiguration.class).constructed().get(1),
                times(3)).setUseHostNameAsBookieID(anyBoolean());

    }

    @Ignore
    @Test
    public void testWithExpand() {
        testCommand("-e");
    }

    @Test
    public void testWithList() {
        testCommand("-l");
    }

    @Test
    public void testWithDelete() throws BookieException {
        testCommand("-d", "-f");
        verify(cookie, times(1))
            .deleteFromRegistrationManager(any(RegistrationManager.class), any(ServerConfiguration.class),
                                           any(Version.class));
    }

    private void testCommand(String... args) {
        AdminCommand cmd = new AdminCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
