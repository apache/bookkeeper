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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieShell.MyCommand;
import org.apache.bookkeeper.bookie.BookieShell.RecoverCmd;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.commands.bookie.LastMarkCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ClusterInfoCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand.Flags;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit test for {@link BookieShell}.
 */
@RunWith(MockitoJUnitRunner.class)
public class BookieShellTest {

    private ClientConfiguration clientConf;
    private BookieShell shell;
    private BookKeeperAdmin admin;
    private RegistrationManager rm;
    private Cookie cookie;
    private Version version;

    private ListBookiesCommand.Flags mockListBookiesFlags;
    private ListBookiesCommand mockListBookiesCommand;
    private MockedStatic<ListBookiesCommand> listBookiesCommandMockedStatic;
    private MockedStatic<MetadataDrivers> metadataDriversMockedStatic;
    private MockedStatic<BookKeeperAdmin> bookKeeperAdminMockedStatic;
    private MockedStatic<ListBookiesCommand.Flags> listBookiesCommandflagsMockedStatic;

    @Before
    public void setup() throws Exception {
        this.shell = new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);

        this.mockListBookiesFlags = spy(new ListBookiesCommand.Flags());
        this.listBookiesCommandflagsMockedStatic = mockStatic(ListBookiesCommand.Flags.class);
        listBookiesCommandflagsMockedStatic.when(() -> ListBookiesCommand.Flags.newFlags())
                .thenReturn(mockListBookiesFlags);
        this.mockListBookiesCommand = spy(new ListBookiesCommand());
        doReturn(true).when(mockListBookiesCommand)
                .apply(any(ServerConfiguration.class), any(ListBookiesCommand.Flags.class));
        listBookiesCommandMockedStatic = mockStatic(ListBookiesCommand.class);
        listBookiesCommandMockedStatic.when(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags))
                .thenReturn(mockListBookiesCommand);

        // construct the bookie shell.
        this.admin = mock(BookKeeperAdmin.class);

        bookKeeperAdminMockedStatic = mockStatic(BookKeeperAdmin.class);
        bookKeeperAdminMockedStatic.when(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)))
                .thenReturn(admin);
        this.clientConf = new ClientConfiguration();
        this.clientConf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        when(admin.getConf()).thenReturn(this.clientConf);
        this.rm = mock(RegistrationManager.class);
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

        metadataDriversMockedStatic = mockStatic(MetadataDrivers.class);
        metadataDriversMockedStatic
                .when(() -> MetadataDrivers.runFunctionWithRegistrationManager(
                        any(ServerConfiguration.class), any(Function.class)))
                .thenAnswer(invocationOnMock -> {
                    Function<RegistrationManager, Object> function = invocationOnMock.getArgument(1);
                    function.apply(rm);
                    return null;
                });
    }

    @After
    public void teardown() throws Exception {
        listBookiesCommandMockedStatic.close();
        listBookiesCommandflagsMockedStatic.close();
        metadataDriversMockedStatic.close();
        bookKeeperAdminMockedStatic.close();
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
        bookKeeperAdminMockedStatic.verify(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)),
                never());
    }

    @Test
    public void testRecoverCmdInvalidBookieAddress() throws Exception {
        RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
        CommandLine cmdLine = parseCommandLine(cmd, "non.valid$$bookie.id");
        assertEquals(-1, cmd.runCmd(cmdLine));
        bookKeeperAdminMockedStatic.verify(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)),
                never());
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
        bookKeeperAdminMockedStatic.verify(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)),
                times(1));
        verify(admin, times(1)).getLedgersContainBookies(any(Set.class));
        verify(admin, times(1)).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecoverLedgerWithRateLimit() throws Exception {
        testRecoverCmdRecoverLedger(
                12345, false, false, false,
                "-force", "-l", "12345", "-rate", "10000", "127.0.0.1:3181");
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
        final PrintStream oldPs = System.err;
        try(ByteArrayOutputStream outContent = new ByteArrayOutputStream()) {
            System.setErr(new PrintStream(outContent));

            RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
            CommandLine cmdLine = parseCommandLine(cmd, args);
            assertEquals(0, cmd.runCmd(cmdLine));
            bookKeeperAdminMockedStatic.verify(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)),
                    times(1));
            verify(admin, times(1))
                .recoverBookieData(eq(ledgerId), any(Set.class), eq(dryrun), eq(skipOpenLedgers));
            verify(admin, times(1)).close();
            if (removeCookies) {
                MetadataDrivers.runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class));
                verify(rm, times(1)).readCookie(any(BookieId.class));
                verify(rm, times(1)).removeCookie(any(BookieId.class), eq(version));
            } else {
                verify(rm, times(0)).readCookie(any(BookieId.class));
                verify(rm, times(0)).removeCookie(any(BookieId.class), eq(version));
            }
            assertFalse("invalid value for option detected:\n" + outContent,
                    outContent.toString().contains("invalid value for option"));
        } finally {
            System.setErr(oldPs);
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
    public void testRecoverWithRateLimit() throws Exception {
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
        final PrintStream oldPs = System.err;
        try(ByteArrayOutputStream outContent = new ByteArrayOutputStream()) {
            System.setErr(new PrintStream(outContent));

            RecoverCmd cmd = (RecoverCmd) shell.commands.get("recover");
            CommandLine cmdLine = parseCommandLine(cmd, args);
            assertEquals(0, cmd.runCmd(cmdLine));
            bookKeeperAdminMockedStatic.verify(() -> BookKeeperAdmin.newBookKeeperAdmin(any(ClientConfiguration.class)),
                    times(1));
            verify(admin, times(1))
                .recoverBookieData(any(Set.class), eq(dryrun), eq(skipOpenLedgers), eq(skipUnrecoverableLedgers));
            verify(admin, times(1)).close();
            if (removeCookies) {
                MetadataDrivers.runFunctionWithRegistrationManager(any(ServerConfiguration.class), any(Function.class));
                verify(rm, times(1)).readCookie(any(BookieId.class));
                verify(rm, times(1)).removeCookie(any(BookieId.class), eq(version));
            } else {
                verify(rm, times(0)).readCookie(any(BookieId.class));
                verify(rm, times(0)).removeCookie(any(BookieId.class), eq(version));
            }
            assertFalse("invalid value for option detected:\n" + outContent,
                    outContent.toString().contains("invalid value for option"));
        } finally {
            System.setErr(oldPs);
        }
    }

    @Test
    public void testLastMarkCmd() throws Exception {
        LastMarkCommand mockLastMarkCommand = mock(LastMarkCommand.class);

        @Cleanup
        MockedStatic<LastMarkCommand> lastMarkCommandMockedStatic = mockStatic(LastMarkCommand.class);
        lastMarkCommandMockedStatic.when(() -> LastMarkCommand.newLastMarkCommand()).thenReturn(mockLastMarkCommand);

        shell.run(new String[] { "lastmark"});
        lastMarkCommandMockedStatic.verify(() -> LastMarkCommand.newLastMarkCommand(), times(1));
        verify(mockLastMarkCommand, times(1))
            .apply(same(shell.bkConf), any(CliFlags.class));
    }

    @Test
    public void testSimpleTestCmd() throws Exception {
        SimpleTestCommand.Flags mockSimpleTestFlags = spy(new SimpleTestCommand.Flags());

        @Cleanup
        MockedStatic<Flags> flagsMockedStatic = mockStatic(Flags.class);
        flagsMockedStatic.when(() -> Flags.newFlags()).thenReturn(mockSimpleTestFlags);

        SimpleTestCommand mockSimpleTestCommand = spy(new SimpleTestCommand());
        doReturn(true).when(mockSimpleTestCommand)
                .apply(any(ServerConfiguration.class), any(SimpleTestCommand.Flags.class));

        @Cleanup
        MockedStatic<SimpleTestCommand> simpleTestCommandMockedStatic = mockStatic(SimpleTestCommand.class);
        simpleTestCommandMockedStatic.when(() -> SimpleTestCommand.newSimpleTestCommand(mockSimpleTestFlags))
                .thenReturn(mockSimpleTestCommand);

        shell.run(new String[] {
            "simpletest",
            "-e", "10",
            "-w", "5",
            "-a", "3",
            "-n", "200"
        });
        simpleTestCommandMockedStatic.verify(() -> SimpleTestCommand.newSimpleTestCommand(mockSimpleTestFlags),
                times(1));
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

        listBookiesCommandMockedStatic.verify(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags)
                , times(0));
    }

    @Test
    public void testListBookiesCmdConflictArgs() throws Exception {
        assertEquals(1, shell.run(new String[] {
            "listbookies", "-rw", "-ro"
        }));
        listBookiesCommandMockedStatic.verify(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags),
                times(0));
    }

    @Test
    public void testListBookiesCmdReadOnly() throws Exception {
        assertEquals(0, shell.run(new String[] {
            "listbookies", "-ro"
        }));

        listBookiesCommandMockedStatic.verify(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags),
                times(1));
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
        listBookiesCommandMockedStatic.verify(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags),
                times(1));
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
        listBookiesCommandMockedStatic.verify(() -> ListBookiesCommand.newListBookiesCommand(mockListBookiesFlags),
                times(1));
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
        ClusterInfoCommand mockClusterInfoCommand = spy(new ClusterInfoCommand());

        @Cleanup
        MockedStatic<ClusterInfoCommand> clusterInfoCommandMockedStatic = mockStatic(ClusterInfoCommand.class);
        clusterInfoCommandMockedStatic.when(() -> ClusterInfoCommand.newClusterInfoCommand())
                .thenReturn(mockClusterInfoCommand);

        doReturn(true).when(mockClusterInfoCommand).apply(same(shell.bkConf), any(CliFlags.class));
        shell.run(new String[]{ "clusterinfo" });
        clusterInfoCommandMockedStatic.verify(() -> ClusterInfoCommand.newClusterInfoCommand(),
                times(1));
    }

}
