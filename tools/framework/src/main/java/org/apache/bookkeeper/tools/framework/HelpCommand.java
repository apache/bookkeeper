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

package org.apache.bookkeeper.tools.framework;

/**
 * Help command to show help information.
 */
class HelpCommand<GlobalFlagsT extends CliFlags, CommandFlagsT extends CliFlags>
        implements Command<GlobalFlagsT> {

    private final Cli<CommandFlagsT> cli;

    HelpCommand(Cli<CommandFlagsT> cli) {
        this.cli = cli;
    }

    @Override
    public String name() {
        return "help";
    }

    @Override
    public String description() {
        return "Display help information about it";
    }

    @Override
    public Boolean apply(GlobalFlagsT globalFlags, String[] args) throws Exception {
        if (args.length == 0) {
            cli.usage();
            return true;
        }

        String cmd = args[0];
        Command command = cli.getCommand(cmd);
        if (null != command) {
            command.usage();
        } else {
            cli.usage("Command \"" + cmd + "\" is not found.");
        }

        return true;
    }

    @Override
    public void usage() {
        cli.console("Help provides help for any command in this cli.");
        cli.console("Simply type '" + cli.cmdPath() + " help [command]' for full details.");
        cli.console("");
        CommandUtils.printUsage(cli.console(), cli.cmdPath() + " help [command] [options]");
    }

}
