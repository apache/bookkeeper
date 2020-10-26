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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
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
 * Unit test for {@link RecoverCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ RecoverCommand.class, MetadataDrivers.class, Cookie.class})
public class RecoverCommandTest extends BookieCommandTestBase {

    private BookieId bookieSocketAddress = BookieId.parse("127.0.0.1:8000");

    @Mock
    private ClientConfiguration clientConfiguration;

    @Mock
    private BookKeeperAdmin bookKeeperAdmin;

    @Mock
    private LedgerMetadata ledgerMetadata;

    @Mock
    private ServerConfiguration serverConfiguration;

    @Mock
    private RegistrationManager registrationManager;

    @Mock
    private Versioned<Cookie> cookieVersioned;

    public RecoverCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(conf);
        PowerMockito.whenNew(ClientConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(clientConfiguration);
        PowerMockito.whenNew(BookKeeperAdmin.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(bookKeeperAdmin);

        mockBkQuery();
        mockDeleteCookie();
        mockDeleteCookies();
        mockBkRecovery();

    }

    private void mockBkQuery() throws BKException, InterruptedException {
        SortedMap<Long, LedgerMetadata> ledgerMetadataSortedMap = new TreeMap<>();
        ledgerMetadataSortedMap.put(1L, ledgerMetadata);
        when(bookKeeperAdmin.getLedgersContainBookies(any())).thenReturn(ledgerMetadataSortedMap);
        ArrayList<BookieId> arrayList = new ArrayList<>();
        arrayList.add(bookieSocketAddress);
        Map<Long, List<BookieId>> map = new HashMap<>();
        map.put(1L, arrayList);
        NavigableMap<Long, ImmutableList<BookieId>> navigableMap = Collections.unmodifiableNavigableMap(
            map.entrySet().stream()
               .collect(TreeMap::new, (m, e) -> m.put(e.getKey(), ImmutableList.copyOf(e.getValue())),
                        TreeMap::putAll));
        doReturn(navigableMap).when(ledgerMetadata).getAllEnsembles();
    }



    private void mockDeleteCookies() throws Exception {
        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, ?> function = invocationOnMock.getArgument(1);
            function.apply(registrationManager);
            return null;
        }).when(MetadataDrivers.class, "runFunctionWithRegistrationManager", any(ServerConfiguration.class),
                any(Function.class));
    }

    private void mockDeleteCookie() throws BookieException {
        PowerMockito.mockStatic(Cookie.class);
        when(Cookie.readFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress)))
            .thenReturn(cookieVersioned);
        Cookie cookie = mock(Cookie.class);
        when(cookieVersioned.getValue()).thenReturn(cookie);
        Version version = mock(Version.class);
        when(cookieVersioned.getVersion()).thenReturn(version);
        doNothing().when(cookie)
                   .deleteFromRegistrationManager(eq(registrationManager), eq(bookieSocketAddress), eq(version));
    }

    private void mockBkRecovery() throws BKException, InterruptedException {
        doNothing().when(bookKeeperAdmin).recoverBookieData(any(), anyBoolean(), anyBoolean());
        when(bookKeeperAdmin.getConf()).thenReturn(clientConfiguration);
    }

    @Test
    public void testBookieListCheck() {
        RecoverCommand cmd = new RecoverCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[] { "-bs", "127.0.0.1:8000,$nonvalidbookieid:8001" }));
    }

    @Test
    public void testQuery() throws Exception {
        RecoverCommand cmd = new RecoverCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-q", "-bs", "127.0.0.1:8000", "-f" }));
        verifyNew(ClientConfiguration.class, times(1)).withArguments(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1)).getLedgersContainBookies(any());
    }

    @Test
    public void testLedgerId() throws Exception {
        RecoverCommand cmd = new RecoverCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-bs", "127.0.0.1:8000", "-f", "-l", "1" }));
        verifyNew(ClientConfiguration.class, times(1)).withArguments(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1))
            .recoverBookieData(anyLong(), any(), anyBoolean(), anyBoolean());
    }

    @Test
    public void testWithLedgerIdAndRemoveCookies() throws Exception {
        RecoverCommand cmd = new RecoverCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-bs", "127.0.0.1:8000", "-f", "-l", "1", "-d" }));
        verifyNew(ClientConfiguration.class, times(1)).withArguments(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1)).recoverBookieData(anyLong(), any(), anyBoolean(), anyBoolean());
        verify(bookKeeperAdmin, times(1)).getConf();
        verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(clientConfiguration));
        verify(cookieVersioned, times(1)).getValue();
    }
}
