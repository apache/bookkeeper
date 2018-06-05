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
package org.apache.bookkeeper.tools.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.Command;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of "bookie" commands.
 */
public class CmdBaseTest {

    @Parameters(commandDescription = "sub command")
    static class SubCommand implements Command {

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

        CmdTest(ServerConfiguration conf) {
            this(conf, new JCommander(), new SubCommand());
        }

        CmdTest(ServerConfiguration conf, JCommander commander, SubCommand subCommand) {
            super("test", conf, commander);
            addSubCommand(subCommand);
        }
    }

    private ServerConfiguration conf;
    private JCommander commander;
    private SubCommand subCommand;
    private CmdTest cmdTest;

    @Before
    public void setup() {
        this.conf = new ServerConfiguration();
        this.commander = spy(new JCommander());
        this.subCommand = spy(new SubCommand());
        this.cmdTest = new CmdTest(conf, commander, subCommand);
    }

    @Test
    public void testParseFailure() throws Exception {
        String[] args = new String[] { "--unknown-flag" };
        assertFalse(cmdTest.run(args));
        verify(commander, times(1)).parse(args);
        verify(commander, times(1)).usage();
        verify(commander, times(0)).getParsedCommand();
    }

    @Test
    public void testUnknownSubCommand() throws Exception {
        String[] args = new String[] { "unknown" };
        assertFalse(cmdTest.run(args));
        verify(commander, times(1)).parse(args);
        verify(commander, times(1)).usage();
        verify(commander, times(0)).getParsedCommand();
    }

    @Test
    public void testSubCommandNoArgs() throws Exception {
        String[] args = new String[] { "subtest" };
        assertTrue(cmdTest.run(args));
        verify(commander, times(1)).parse(args);
        verify(commander, times(0)).usage();
        verify(commander, times(1)).getParsedCommand();
        verify(commander, times(1)).getCommands();
        verify(subCommand, times(1)).run(eq(conf));
        assertEquals(-1, subCommand.value);
    }

    @Test
    public void testSubCommandWithArgs() throws Exception {
        String[] args = new String[] { "subtest", "--value", "10" };
        assertTrue(cmdTest.run(args));
        verify(commander, times(1)).parse(args);
        verify(commander, times(0)).usage();
        verify(commander, times(1)).getParsedCommand();
        verify(commander, times(1)).getCommands();
        verify(subCommand, times(1)).run(eq(conf));
        assertEquals(10, subCommand.value);
    }
}
