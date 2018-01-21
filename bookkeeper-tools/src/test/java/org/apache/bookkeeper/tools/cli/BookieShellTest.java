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
package org.apache.bookkeeper.tools.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.commands.CmdBase;
import org.apache.bookkeeper.tools.cli.commands.CmdBookie;
import org.apache.bookkeeper.tools.cli.commands.CmdClient;
import org.apache.bookkeeper.tools.cli.commands.CmdCluster;
import org.apache.bookkeeper.tools.cli.commands.CmdMetadata;
import org.apache.bookkeeper.tools.cli.helpers.Command;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link BookieShell}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ BookieShell.class, CmdBase.class })
@PowerMockIgnore("org.apache.log4j.*")
@Slf4j
public class BookieShellTest {

    @Parameters(commandDescription = "sub test command")
    static class SubTestCommand implements Command {

        @Parameter(names = "--value")
        private int value = -1;

        @Override
        public String name() {
            return "subtest";
        }

        @Override
        public void run(ServerConfiguration conf) throws Exception {
        }
    }

    @Parameters(commandDescription = "test command")
    static class CmdTest extends CmdBase {

        public CmdTest(ServerConfiguration conf) {
            this(conf, new JCommander());
        }

        public CmdTest(ServerConfiguration conf, JCommander commander) {
            super("test", conf, commander);
            addSubCommand(new SubTestCommand());
        }

        @Override
        public boolean run(String[] args) {
            return super.run(args);
        }
    }

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private ServerConfiguration conf;
    private JCommander commander;
    private JCommander subCommander;
    private BookieShell shell;
    private CmdTest cmdTest;
    private SubTestCommand subCmdTest;

    @Before
    public void setup() throws Exception {
        this.conf = spy(new ServerConfiguration());
        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments()
            .thenReturn(conf);

        this.commander = spy(new JCommander());
        PowerMockito.mockStatic(BookieShell.class);
        when(BookieShell.newJCommander()).thenReturn(commander);

        this.subCommander = spy(new JCommander());
        this.cmdTest = spy(new CmdTest(conf, subCommander));
        when(BookieShell.newCommandInstance(eq(CmdTest.class), eq(conf)))
            .thenReturn(cmdTest);
        when(BookieShell.newCommandInstance(eq(CmdClient.class), eq(conf)))
            .thenReturn(new CmdClient(conf));
        when(BookieShell.newCommandInstance(eq(CmdCluster.class), eq(conf)))
            .thenReturn(new CmdCluster(conf));
        when(BookieShell.newCommandInstance(eq(CmdMetadata.class), eq(conf)))
            .thenReturn(new CmdMetadata(conf));
        when(BookieShell.newCommandInstance(eq(CmdBookie.class), eq(conf)))
            .thenReturn(new CmdBookie(conf));

        PowerMockito.doCallRealMethod().when(
            BookieShell.class, "registerSubcommand", eq("test"), eq(CmdTest.class));
        BookieShell.registerSubcommand("test", CmdTest.class);


        this.subCmdTest = spy(new SubTestCommand());
        PowerMockito.whenNew(SubTestCommand.class).withNoArguments()
            .thenReturn(subCmdTest);

        this.shell = spy(new BookieShell());
    }

    @Test
    public void testNoArgs() throws ConfigurationException {
        BookieShell.unregisterSubcommand("test");

        assertFalse(shell.runArgs());
        verify(shell, times(1)).setupShell();
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testHelpShort() throws ConfigurationException {
        assertFalse(shell.runArgs("-h"));
        verify(shell, times(1)).setupShell();
        assertNull(shell.getShellArgs().getConfigFile());
        assertTrue(shell.getShellArgs().isHelp());
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testHelpLong() throws Exception {
        assertFalse(shell.runArgs("--help"));
        verify(shell, times(1)).setupShell();
        assertNull(shell.getShellArgs().getConfigFile());
        assertTrue(shell.getShellArgs().isHelp());
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testUnknownCommand() throws Exception {
        assertFalse(shell.runArgs("unknown"));
        verify(shell, times(1)).setupShell();
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testConfShort() throws Exception {
        File confFile = testDir.newFile();
        confFile.createNewFile();
        assertFalse(shell.runArgs("-c", confFile.getAbsolutePath()));
        assertEquals(confFile.getAbsolutePath(), shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(conf, times(1)).loadConf(
            eq(Paths.get(confFile.getAbsolutePath()).toUri().toURL()));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testConfLong() throws Exception {
        File confFile = testDir.newFile();
        confFile.createNewFile();
        assertFalse(shell.runArgs("--conf", confFile.getAbsolutePath()));
        assertEquals(confFile.getAbsolutePath(), shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(conf, times(1)).loadConf(
            eq(Paths.get(confFile.getAbsolutePath()).toUri().toURL()));
        verify(commander, times(1)).usage();
    }

    @Test
    public void testCmdTestNoSubCommand() throws Exception {
        assertFalse(shell.runArgs("test"));
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(commander, times(0)).usage();
        verify(cmdTest, times(1)).run(eq(new String[0]));
    }

    @Test
    public void testCmdTestWithUnknownSubCommand() throws Exception {
        assertFalse(shell.runArgs("test", "unknown"));
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(commander, times(0)).usage();
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(cmdTest, times(1)).run(eq(new String[] { "unknown" }));
        assertEquals(-1, subCmdTest.value);
        assertSame(conf, cmdTest.getConf());
        verify(subCommander, times(1)).usage();
    }

    @Test
    public void testCmdTestWithSubCommandNoArgs() throws Exception {
        assertTrue(shell.runArgs("test", "subtest"));
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(commander, times(0)).usage();
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(cmdTest, times(1)).run(eq(new String[] { "subtest" }));
        assertEquals(-1, subCmdTest.value);
        assertSame(conf, cmdTest.getConf());
        verify(subCommander, times(0)).usage();
    }

    @Test
    public void testCmdTestWithSubCommandWithArgs() throws Exception {
        assertTrue(shell.runArgs("test", "subtest", "--value", "10"));
        assertNull(shell.getShellArgs().getConfigFile());
        assertFalse(shell.getShellArgs().isHelp());
        verify(commander, times(0)).usage();
        verify(conf, times(0)).loadConf(any(URL.class));
        verify(cmdTest, times(1)).run(eq(new String[] { "subtest", "--value", "10" }));
        assertSame(conf, cmdTest.getConf());
        verify(subCommander, times(0)).usage();
    }
}
