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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.google.common.collect.Maps;
import java.util.Set;
import java.util.SortedMap;
import org.apache.bookkeeper.bookie.BookieShell.MyCommand;
import org.apache.bookkeeper.bookie.BookieShell.RecoverCmd;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link BookieShell}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BookieShell.class)
public class BookieShellTest {

    private ClientConfiguration clientConf;
    private BookieShell shell;
    private BookKeeperAdmin admin;
    private ZKRegistrationManager rm;
    private Cookie cookie;
    private Version version;

    @Before
    public void setup() throws Exception {
        this.shell = new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);
        this.admin = PowerMockito.mock(BookKeeperAdmin.class);
        whenNew(BookKeeperAdmin.class)
            .withParameterTypes(ClientConfiguration.class)
            .withArguments(any(ClientConfiguration.class))
            .thenReturn(admin);
        this.clientConf = new ClientConfiguration();
        when(admin.getConf()).thenReturn(this.clientConf);
        this.rm = PowerMockito.mock(ZKRegistrationManager.class);
        this.cookie = Cookie.newBuilder()
            .setBookieHost("127.0.0.1:3181")
            .setInstanceId("xyz")
            .setJournalDirs("/path/to/journal/dir")
            .setLedgerDirs("/path/to/journal/dir")
            .setLayoutVersion(Cookie.CURRENT_COOKIE_LAYOUT_VERSION)
            .build();
        this.version = new LongVersion(1L);
        when(rm.readCookie(anyString()))
            .thenReturn(new Versioned<>(cookie.toString().getBytes(UTF_8), version));
        whenNew(ZKRegistrationManager.class)
            .withNoArguments()
            .thenReturn(rm);
    }

    private static CommandLine parseCommandLine(MyCommand cmd, String... args) throws ParseException {
        BasicParser parser = new BasicParser();
        return parser.parse(cmd.getOptions(), args);
    }

    @Test
    public void testRecoverCmdMissingArgument() throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd);
        try {
            cmd.runCmd(cmdLine);
            fail("should fail running command when the arguments are missing");
        } catch (MissingArgumentException e) {
            // expected
        }
        PowerMockito.verifyNew(BookKeeperAdmin.class, never()).withArguments(any(ClientConfiguration.class));
    }

    @Test
    public void testRecoverCmdInvalidBookieAddress() throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, "127.0.0.1");
        assertEquals(-1, cmd.runCmd(cmdLine));
        PowerMockito.verifyNew(BookKeeperAdmin.class, never()).withArguments(any(ClientConfiguration.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoverCmdQuery() throws Exception {
        SortedMap<Long, LedgerMetadata> ledgersContainBookies = Maps.newTreeMap();
        when(admin.getLedgersContainBookies(any(Set.class)))
            .thenReturn(ledgersContainBookies);

        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, "-force", "-q", "127.0.0.1:3181");
        assertEquals(0, cmd.runCmd(cmdLine));
        PowerMockito
            .verifyNew(BookKeeperAdmin.class, times(1))
            .withArguments(any(ClientConfiguration.class));
        verify(admin, times(1)).getLedgersContainBookies(any(Set.class));
        verify(admin, times(1)).close();
    }

    @Test
    public void testRecoverCmdRecoverLedgerDefault() throws Exception {
        // default behavior
        testRecoverCmdRecoverLedger(
            12345, false, false, false,
            "-force", "-l", "12345", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverLedgerDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecoverLedger(
            12345, false, false, true,
            "-force", "-l", "12345", "-deleteCookie", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverLedgerSkipOpenLedgersDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecoverLedger(
            12345, false, true, true,
            "-force", "-l", "12345", "-deleteCookie", "-skipOpenLedgers", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverLedgerDryrun() throws Exception {
        // dryrun
        testRecoverCmdRecoverLedger(
            12345, true, false, false,
            "-force", "-l", "12345", "-dryrun", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverLedgerDryrunDeleteCookie() throws Exception {
        // dryrun & removeCookie : removeCookie should be false
        testRecoverCmdRecoverLedger(
            12345, true, false, false,
            "-force", "-l", "12345", "-dryrun", "-deleteCookie", "127.0.0.1:3181");
    }

    @SuppressWarnings("unchecked")
    void testRecoverCmdRecoverLedger(long ledgerId,
                                     boolean dryrun,
                                     boolean skipOpenLedgers,
                                     boolean removeCookies,
                                     String... args) throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, args);
        assertEquals(0, cmd.runCmd(cmdLine));
        PowerMockito
            .verifyNew(BookKeeperAdmin.class, times(1))
            .withArguments(any(ClientConfiguration.class));
        verify(admin, times(1))
            .recoverBookieData(eq(ledgerId), any(Set.class), eq(dryrun), eq(skipOpenLedgers));
        verify(admin, times(1)).close();
        if (removeCookies) {
            PowerMockito
                .verifyNew(ZKRegistrationManager.class, times(1))
                .withNoArguments();
            verify(rm, times(1)).initialize(
                any(ServerConfiguration.class), any(RegistrationListener.class), eq(NullStatsLogger.INSTANCE));
            verify(rm, times(1)).readCookie(anyString());
            verify(rm, times(1)).removeCookie(anyString(), eq(version));
        } else {
            PowerMockito
                .verifyNew(ZKRegistrationManager.class, never())
                .withNoArguments();
        }
    }

    @Test
    public void testRecoverCmdRecoverDefault() throws Exception {
        // default behavior
        testRecoverCmdRecover(
            false, false, false,
            "-force", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            false, false, true,
            "-force", "-deleteCookie", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverSkipOpenLedgersDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            false, true, true,
            "-force", "-deleteCookie", "-skipOpenLedgers", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDryrun() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            true, false, false,
            "-force", "-dryrun", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDryrunDeleteCookie() throws Exception {
        // dryrun & removeCookie : removeCookie should be false
        testRecoverCmdRecover(
            true, false, false,
            "-force", "-dryrun", "-deleteCookie", "127.0.0.1:3181");
    }

    @SuppressWarnings("unchecked")
    void testRecoverCmdRecover(boolean dryrun,
                               boolean skipOpenLedgers,
                               boolean removeCookies,
                               String... args) throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, args);
        assertEquals(0, cmd.runCmd(cmdLine));
        PowerMockito
            .verifyNew(BookKeeperAdmin.class, times(1))
            .withArguments(any(ClientConfiguration.class));
        verify(admin, times(1))
            .recoverBookieData(any(Set.class), eq(dryrun), eq(skipOpenLedgers));
        verify(admin, times(1)).close();
        if (removeCookies) {
            PowerMockito
                .verifyNew(ZKRegistrationManager.class, times(1))
                .withNoArguments();
            verify(rm, times(1)).initialize(
                any(ServerConfiguration.class), any(RegistrationListener.class), eq(NullStatsLogger.INSTANCE));
            verify(rm, times(1)).readCookie(anyString());
            verify(rm, times(1)).removeCookie(anyString(), eq(version));
        } else {
            PowerMockito
                .verifyNew(ZKRegistrationManager.class, never())
                .withNoArguments();
        }
    }
}
