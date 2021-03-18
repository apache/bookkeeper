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
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.function.Function;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.BookKeeperConstants;
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
 * Unit test for {@link AdminCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ AdminCommand.class, MetadataDrivers.class, Cookie.class, Bookie.class, RegistrationManager.class })
public class AdminCommandTest extends BookieCommandTestBase {

    @Mock
    private ServerConfiguration serverConfiguration;

    @Mock
    private Versioned<Cookie> cookieVersioned;

    @Mock
    private Cookie cookie;

    private BookieId bookieSocketAddress = BookieId.parse("localhost:9000");

    public AdminCommandTest() throws IOException {
        super(3, 3);
    }

    public void createIndex() throws IOException {
        String[] indexDirs = new String[3];
        for (int i = 0; i < indexDirs.length; i++) {
            File dir = this.testDir.newFile();
            dir.mkdirs();
            indexDirs[i] = dir.getAbsolutePath();
        }
        this.conf.setIndexDirName(indexDirs);

    }
    @Override
    public void setup() throws Exception {
        super.setup();
        createIndex();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(serverConfiguration);
        PowerMockito.mockStatic(Cookie.class);
        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.mockStatic(Bookie.class);

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
        PowerMockito.when(Bookie.getCurrentDirectories(any())).thenReturn(files);
    }

    private void mockUpdateBookieIdInCookie() throws Exception {
        RegistrationManager registrationManager = mock(RegistrationManager.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, ?> f = invocationOnMock.getArgument(1);
            f.apply(registrationManager);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithRegistrationManager", any(ServerConfiguration.class),
                any(Function.class));

        PowerMockito.when(Bookie.getBookieId(eq(serverConfiguration))).thenReturn(bookieSocketAddress);
        PowerMockito.when(Cookie.readFromRegistrationManager(eq(registrationManager), eq(serverConfiguration)))
                    .thenReturn(cookieVersioned);
        PowerMockito.when(Cookie.readFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress)))
                    .thenReturn(cookieVersioned);
        when(cookieVersioned.getValue()).thenReturn(cookie);
        Cookie.Builder builder = mock(Cookie.Builder.class);
        PowerMockito.when(Cookie.newBuilder(eq(cookie))).thenReturn(builder);
        PowerMockito.when(builder.setBookieId(anyString())).thenReturn(builder);
        when(builder.build()).thenReturn(cookie);

        PowerMockito.when(serverConfiguration.setUseHostNameAsBookieID(anyBoolean())).thenReturn(serverConfiguration);
        PowerMockito.when(Cookie.readFromRegistrationManager(eq(registrationManager), eq(serverConfiguration)))
                    .thenReturn(cookieVersioned);

        Version version = mock(Version.class);
        when(cookieVersioned.getVersion()).thenReturn(version);
        when(cookieVersioned.getValue()).thenReturn(cookie);
        doNothing().when(cookie)
                   .deleteFromRegistrationManager(eq(registrationManager), eq(serverConfiguration), eq(version));

        doNothing().when(cookie).writeToDirectory(any(File.class));
        doNothing().when(cookie)
                   .writeToRegistrationManager(eq(registrationManager), eq(serverConfiguration), eq(Version.NEW));

        doNothing().when(cookie)
                   .deleteFromRegistrationManager(eq(registrationManager), any(ServerConfiguration.class), eq(version));
    }

    private void mockVerifyCookie() throws IOException, BookieException.InvalidCookieException {
        PowerMockito.when(Cookie.readFromDirectory(any(File.class))).thenReturn(cookie);
        doNothing().when(cookie).verify(any(Cookie.class));
    }

    private void mockExpandStorage() throws Exception {
        MetadataBookieDriver metadataBookieDriver = mock(MetadataBookieDriver.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<MetadataBookieDriver, ?> f = invocationOnMock.getArgument(1);
            f.apply(metadataBookieDriver);
            return true;
        }).when(MetadataDrivers.class, "runFunctionWithMetadataBookieDriver", any(ServerConfiguration.class),
                any(Function.class));
        PowerMockito.doNothing()
                    .when(Bookie.class, "checkEnvironmentWithStorageExpansion", any(ServerConfiguration.class),
                          eq(metadataBookieDriver), any(), any());
    }

    private void mockListOrDeleteCookies() throws UnknownHostException {

        when(Bookie.getBookieId(any(ServerConfiguration.class))).thenReturn(bookieSocketAddress);
    }

    @Test
    public void testWithoutAnyFlags() {
        AdminCommand cmd = new AdminCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] {""}));
    }

    @Test
    public void testWithHostName() throws Exception {
        conf.setUseHostNameAsBookieID(true);
        testCommand("-host");
        verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        verify(serverConfiguration, times(3)).setUseHostNameAsBookieID(anyBoolean());

        verify(cookie, times(2)).verify(any(Cookie.class));
    }

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
