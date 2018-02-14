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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.Command;

/**
 * The command base for other sub commands to extend.
 */
@Slf4j
public abstract class CmdBase {

    // Parameters defined for this command

    @Parameter(names = { "-h", "--help" }, help = true, hidden = true)
    private boolean help;

    // Parameters defined for this command (end)

    protected final JCommander commander;
    @Getter(AccessLevel.PUBLIC)
    protected final ServerConfiguration conf;

    protected CmdBase(String cmdName, ServerConfiguration conf) {
        this(cmdName, conf, new JCommander());
    }

    protected CmdBase(String cmdName, ServerConfiguration conf, JCommander commander) {
        this.conf = conf;
        this.commander = commander;
        this.commander.setProgramName("bookie-shell " + cmdName);
    }

    protected void addSubCommand(Command command) {
        this.commander.addCommand(command.name(), command);
    }

    public boolean run(String[] args) {
        try {
            commander.parse(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            commander.usage();
            return false;
        }

        String cmd = commander.getParsedCommand();
        if (null == cmd) {
            commander.usage();
            return false;
        }

        JCommander cmdObj = commander.getCommands().get(cmd);
        Command subCmd = (Command) cmdObj.getObjects().get(0);

        try {
            if (subCmd.validateArgs()) {
                subCmd.run(conf);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            System.err.println("Failed to execute command '" + cmd + "' : " + e.getMessage());
            e.printStackTrace();
            System.err.println();
            commander.usage();
            return false;
        }

    }
}
