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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.google.common.collect.Maps;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import org.apache.bookkeeper.bookie.BookieShell.MyCommand;
import org.apache.bookkeeper.bookie.BookieShell.RecoverCmd;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.commands.bookie.LastMarkCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ClusterInfoCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.RecoverCommand;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
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
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link BookieShell}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*"})
@PrepareForTest({ BookieShell.class, MetadataDrivers.class, RecoverCommand.class })
public class BookieShellTest {

    private ClientConfiguration clientConf;
    private BookieShell shell;
    private BookKeeperAdmin admin;
    private RegistrationManager rm;
    private MetadataBookieDriver driver;
    private Cookie cookie;
    private Version version;

    // commands
    private LastMarkCommand mockLastMarkCommand;
    private SimpleTestCommand.Flags mockSimpleTestFlags;
    private SimpleTestCommand mockSimpleTestCommand;
    private ListBookiesCommand.Flags mockListBookiesFlags;
    private ListBookiesCommand mockListBookiesCommand;
    private ClusterInfoCommand mockClusterInfoCommand;

    @Before
    public void setup() throws Exception {
        // setup the required mocks before constructing bookie shell.
        this.mockLastMarkCommand = mock(LastMarkCommand.class);
        whenNew(LastMarkCommand.class)
            .withNoArguments()
            .thenReturn(mockLastMarkCommand);

        // setup the mocks for simple test command
        this.mockSimpleTestFlags = spy(new SimpleTestCommand.Flags());
        whenNew(SimpleTestCommand.Flags.class)
            .withNoArguments()
            .thenReturn(mockSimpleTestFlags);

        this.mockSimpleTestCommand = spy(new SimpleTestCommand());
        doReturn(true).when(mockSimpleTestCommand)
            .apply(any(ServerConfiguration.class), any(SimpleTestCommand.Flags.class));
        whenNew(SimpleTestCommand.class)
            .withParameterTypes(SimpleTestCommand.Flags.class)
            .withArguments(mockSimpleTestFlags)
            .thenReturn(mockSimpleTestCommand);

        // setup the mocks for list bookies command
        this.mockListBookiesFlags = spy(new ListBookiesCommand.Flags());
        whenNew(ListBookiesCommand.Flags.class)
            .withNoArguments()
            .thenReturn(mockListBookiesFlags);

        this.mockListBookiesCommand = spy(new ListBookiesCommand());
        doReturn(true).when(mockListBookiesCommand)
            .apply(any(ServerConfiguration.class), any(ListBookiesCommand.Flags.class));
        whenNew(ListBookiesCommand.class)
            .withParameterTypes(ListBookiesCommand.Flags.class)
            .withArguments(mockListBookiesFlags)
            .thenReturn(mockListBookiesCommand);

        this.mockClusterInfoCommand = spy(new ClusterInfoCommand());
        whenNew(ClusterInfoCommand.class)
                .withNoArguments()
                .thenReturn(mockClusterInfoCommand);

        // construct the bookie shell.
        this.shell = new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);
        this.admin = PowerMockito.mock(BookKeeperAdmin.class);
        whenNew(BookKeeperAdmin.class)
            .withParameterTypes(ClientConfiguration.class)
            .withArguments(any(ClientConfiguration.class))
            .thenReturn(admin);
        this.clientConf = new ClientConfiguration();
        this.clientConf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        when(admin.getConf()).thenReturn(this.clientConf);
        this.rm = PowerMockito.mock(RegistrationManager.class);
        this.cookie = Cookie.newBuilder()
            .setBookieId("127.0.0.1:3181")
            .setInstanceId("xyz")
            .setJournalDirs("/path/to/journal/dir")
            .setLedgerDirs("/path/to/journal/dir")
            .setLayoutVersion(Cookie.CURRENT_COOKIE_LAYOUT_VERSION)
            .build();
        this.version = new LongVersion(1L);
        when(rm.readCookie(any(BookieId.class)))
            .thenReturn(new Versioned<>(cookie.toString().getBytes(UTF_8), version));

        this.driver = mock(MetadataBookieDriver.class);
        when(driver.createRegistrationManager())
            .thenReturn(rm);

        PowerMockito.mockStatic(MetadataDrivers.class);
        PowerMockito.doAnswer(invocationOnMock -> {
            Function<RegistrationManager, Object> function = invocationOnMock.getArgument(1);
            function.apply(rm);
            return null;
        }).when(
            MetadataDrivers.class,
            "runFunctionWithRegistrationManager",
            any(ServerConfiguration.class),
            any(Function.class)
        );
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
        CommandLine cmdLine = parseCommandLine(cmd, "non.valid$$bookie.id");
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
            PowerMockito.verifyStatic(MetadataDrivers.class);
            MetadataDrivers.runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class));
            verify(rm, times(1)).readCookie(any(BookieId.class));
            verify(rm, times(1)).removeCookie(any(BookieId.class), eq(version));
        } else {
            verify(rm, times(0)).readCookie(any(BookieId.class));
            verify(rm, times(0)).removeCookie(any(BookieId.class), eq(version));
        }
    }

    @Test
    public void testRecoverCmdRecoverDefault() throws Exception {
        // default behavior
        testRecoverCmdRecover(
            false, false, false, false,
            "-force", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            false, false, true, false,
            "-force", "-deleteCookie", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverSkipOpenLedgersDeleteCookie() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            false, true, true, false,
            "-force", "-deleteCookie", "-skipOpenLedgers", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDryrun() throws Exception {
        // dryrun
        testRecoverCmdRecover(
            true, false, false, false,
            "-force", "-dryrun", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverDryrunDeleteCookie() throws Exception {
        // dryrun & removeCookie : removeCookie should be false
        testRecoverCmdRecover(
            true, false, false, false,
            "-force", "-dryrun", "-deleteCookie", "127.0.0.1:3181");
    }

    @Test
    public void testRecoverCmdRecoverSkipUnrecoverableLedgers() throws Exception {
        // skipUnrecoverableLedgers
        testRecoverCmdRecover(
            false, false, false, true,
            "-force", "-sku", "127.0.0.1:3181");
    }

    @SuppressWarnings("unchecked")
    void testRecoverCmdRecover(boolean dryrun,
                               boolean skipOpenLedgers,
                               boolean removeCookies,
                               boolean skipUnrecoverableLedgers,
                               String... args) throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, args);
        assertEquals(0, cmd.runCmd(cmdLine));
        PowerMockito
            .verifyNew(BookKeeperAdmin.class, times(1))
            .withArguments(any(ClientConfiguration.class));
        verify(admin, times(1))
            .recoverBookieData(any(Set.class), eq(dryrun), eq(skipOpenLedgers), eq(skipUnrecoverableLedgers));
        verify(admin, times(1)).close();
        if (removeCookies) {
            PowerMockito.verifyStatic(MetadataDrivers.class);
            MetadataDrivers.runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class));
            verify(rm, times(1)).readCookie(any(BookieId.class));
            verify(rm, times(1)).removeCookie(any(BookieId.class), eq(version));
        } else {
            verify(rm, times(0)).readCookie(any(BookieId.class));
            verify(rm, times(0)).removeCookie(any(BookieId.class), eq(version));
        }
    }

    @Test
    public void testLastMarkCmd() throws Exception {
        shell.run(new String[] { "lastmark"});
        verifyNew(LastMarkCommand.class, times(1)).withNoArguments();
        verify(mockLastMarkCommand, times(1))
            .apply(same(shell.bkConf), any(CliFlags.class));
    }

    @Test
    public void testSimpleTestCmd() throws Exception {
        shell.run(new String[] {
            "simpletest",
            "-e", "10",
            "-w", "5",
            "-a", "3",
            "-n", "200"
        });
        verifyNew(SimpleTestCommand.class, times(1))
            .withArguments(same(mockSimpleTestFlags));
        verify(mockSimpleTestCommand, times(1))
            .apply(same(shell.bkConf), same(mockSimpleTestFlags));
        verify(mockSimpleTestFlags, times(1)).ensembleSize(eq(10));
        verify(mockSimpleTestFlags, times(1)).writeQuorumSize(eq(5));
        verify(mockSimpleTestFlags, times(1)).ackQuorumSize(eq(3));
        verify(mockSimpleTestFlags, times(1)).numEntries(eq(200));
    }

    @Test
    public void testListBookiesCmdNoArgs() throws Exception {
        assertEquals(1, shell.run(new String[] {
            "listbookies"
        }));
        verifyNew(ListBookiesCommand.class, times(0)).withNoArguments();
    }

    @Test
    public void testListBookiesCmdConflictArgs() throws Exception {
        assertEquals(1, shell.run(new String[] {
            "listbookies", "-rw", "-ro"
        }));
        verifyNew(ListBookiesCommand.class, times(0)).withNoArguments();
    }

    @Test
    public void testListBookiesCmdReadOnly() throws Exception {
        assertEquals(0, shell.run(new String[] {
            "listbookies", "-ro"
        }));
        verifyNew(ListBookiesCommand.class, times(1))
            .withArguments(same(mockListBookiesFlags));
        verify(mockListBookiesCommand, times(1))
            .apply(same(shell.bkConf), same(mockListBookiesFlags));
        verify(mockListBookiesFlags, times(1)).readonly(true);
        verify(mockListBookiesFlags, times(1)).readwrite(false);
        verify(mockListBookiesFlags, times(1)).all(false);
    }

    @Test
    public void testListBookiesCmdReadWrite() throws Exception {
        assertEquals(0, shell.run(new String[] {
            "listbookies", "-rw"
        }));
        verifyNew(ListBookiesCommand.class, times(1))
            .withArguments(same(mockListBookiesFlags));
        verify(mockListBookiesCommand, times(1))
            .apply(same(shell.bkConf), same(mockListBookiesFlags));
        verify(mockListBookiesFlags, times(1)).readonly(false);
        verify(mockListBookiesFlags, times(1)).readwrite(true);
        verify(mockListBookiesFlags, times(1)).all(false);
    }

    @Test
    public void testListBookiesCmdAll() throws Exception {
        assertEquals(0, shell.run(new String[] {
            "listbookies", "-a"
        }));
        verifyNew(ListBookiesCommand.class, times(1))
            .withArguments(same(mockListBookiesFlags));
        verify(mockListBookiesCommand, times(1))
            .apply(same(shell.bkConf), same(mockListBookiesFlags));
        verify(mockListBookiesFlags, times(1)).readonly(false);
        verify(mockListBookiesFlags, times(1)).readwrite(false);
        verify(mockListBookiesFlags, times(1)).all(true);
    }

    @Test
    public void testForceAuditChecksWithNoArgs() throws Exception {
        assertEquals(-1, shell.run(new String[] {
                "forceauditchecks"
        }));
    }

    @Test
    public void testForceAuditChecksWithSomeArgs() throws Exception {
        assertEquals(0, shell.run(new String[] {
                "forceauditchecks", "-calc"
        }));
    }

    @Test
    public void testForceAuditChecksWithAllArgs() throws Exception {
        assertEquals(0, shell.run(new String[] {
                "forceauditchecks", "-calc", "-rc", "-ppc"
        }));
    }

    @Test
    public void testClusterInfoCmd() throws Exception {
        doReturn(true).when(mockClusterInfoCommand).apply(same(shell.bkConf), any(CliFlags.class));
        shell.run(new String[]{ "clusterinfo" });
        verifyNew(ClusterInfoCommand.class, times(1)).withNoArguments();
    }

}
