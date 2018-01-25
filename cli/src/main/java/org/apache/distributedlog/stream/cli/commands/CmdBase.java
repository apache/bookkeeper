/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.stream.cli.commands;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.clients.config.StorageClientSettings;

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
    protected final StorageClientSettings.Builder settingsBuilder;

    protected CmdBase(String cmdName, StorageClientSettings.Builder settingsBuilder) {
        this(cmdName, settingsBuilder, new JCommander());
    }

    protected CmdBase(String cmdName, StorageClientSettings.Builder settingsBuilder, JCommander commander) {
        this.settingsBuilder = settingsBuilder;
        this.commander = commander;
        this.commander.setProgramName("bookie-shell " + cmdName);
    }

    protected void addSubCommand(SubCommand subCommand) {
        this.commander.addCommand(subCommand.name(), subCommand);
    }

    public boolean run(String namespace, String[] args) {
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
        SubCommand subCmd = (SubCommand) cmdObj.getObjects().get(0);


        try {
            subCmd.run(namespace, settingsBuilder.build());
            return true;
        } catch (Exception e) {
            System.err.println("Failed to execute command '" + cmd + "' : " + e.getMessage());
            e.printStackTrace();
            System.err.println();
            commander.usage();
            return false;
        }

    }
}
