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

import com.beust.jcommander.JCommander;
import com.google.common.base.Strings;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tools.framework.CliSpec.Builder;

/**
 * Cli to execute {@link CliSpec}.
 */
@Slf4j
public class Cli<CliFlagsT extends CliFlags> {

    private final CliSpec<CliFlagsT> spec;
    private final JCommander commander;
    @Getter(AccessLevel.PACKAGE)
    private final Map<String, Command> commandMap;
    private final Function<CliFlagsT, Boolean> runFunc;
    private final String cmdPath;
    private final PrintStream console;

    public Cli(CliSpec<CliFlagsT> spec) {
        this.spec = updateSpecWithDefaultValues(spec);
        this.cmdPath = getCmdPath(spec);
        this.commandMap = new TreeMap<>();
        this.console = setupConsole(spec);
        this.commander = setupCli(cmdPath, spec, commandMap);
        if (null == spec.runFunc()) {
            this.runFunc = (args) -> {
                usage();
                return false;
            };
        } else {
            this.runFunc = spec.runFunc();
        }

        // inject default commands if needed
        if (!spec.commands().isEmpty() && !hasCommand("help")) {
            commandMap.put("help", new HelpCommand<>(this));
        }
    }

    static <CliFlagsT extends CliFlags> String getCmdPath(CliSpec<CliFlagsT> spec) {
        if (Strings.isNullOrEmpty(spec.parent())) {
            return spec.name();
        } else {
            return spec.parent() + " " + spec.name();
        }
    }

    static <CliFlagsT extends CliFlags> PrintStream setupConsole(CliSpec<CliFlagsT> spec) {
        if (null == spec.console()) {
            return System.out;
        } else {
            return spec.console();
        }
    }

    CliSpec<CliFlagsT> updateSpecWithDefaultValues(CliSpec<CliFlagsT> spec) {
        Builder<CliFlagsT> builder = CliSpec.newBuilder(spec);
        if (Strings.isNullOrEmpty(spec.usage())) {
            // simple command
            if (spec.commands().isEmpty()) {
                builder.withUsage(
                    String.format(
                        "%s [flags] " + spec.argumentsUsage(),
                        getCmdPath(spec)));
            // command group
            } else {
                builder.withUsage(
                    String.format(
                        "%s [flags] [command] [command options]",
                        getCmdPath(spec)));
            }
        }

        if (!spec.commands().isEmpty() && Strings.isNullOrEmpty(spec.tailer())) {
            builder.withTailer(
                String.format(
                      "Use \"%s [command] --help\" or \"%s help [command]\" for "
                    + "more information about a command",
                    getCmdPath(spec), getCmdPath(spec)));
        }
        return builder.build();
    }

    String name() {
        return spec.name();
    }

    String cmdPath() {
        return cmdPath;
    }

    boolean hasCommand(String command) {
        return commandMap.containsKey(command);
    }

    Command getCommand(String command) {
        return commandMap.get(command);
    }

    static <CliFlagsT extends CliFlags> JCommander setupCli(String cmdPath,
                                                            CliSpec<CliFlagsT> spec,
                                                            Map<String, Command> commandMap) {
        JCommander commander = new JCommander();
        commander.setProgramName(cmdPath);
        if (null != spec.flags()) {
            commander.addObject(spec.flags());
        }
        for (Command cmd : spec.commands()) {
            commandMap.put(cmd.name(), cmd);
        }
        // trigger command to generate help information
        StringBuilder usageBuilder = new StringBuilder();
        commander.usage(usageBuilder);
        return commander;
    }

    protected void console(String msg) {
        console().println(msg);
    }

    protected PrintStream console() {
        return console;
    }

    void usage() {
        usage(null);
    }

    void usage(String errorMsg) {
        if (Strings.isNullOrEmpty(errorMsg)) {
            CommandUtils.printDescription(console(), 0, 0, spec.description());
            console("");
        } else {
            CommandUtils.printDescription(console(), 0, 0, "Error : " + errorMsg);
            console("");
        }

        // usage section
        CommandUtils.printUsage(console(), spec.usage());

        // command section
        CommandUtils.printAvailableCommands(commandMap, console());

        // flags section
        if (!spec.isCommandGroup()) {
            CommandUtils.printAvailableFlags(commander, console());
        }

        if (!spec.commands().isEmpty()) {
            // tailer section
            CommandUtils.printDescription(console(), 0, 0, spec.tailer());
        }
    }

    @SuppressWarnings("unchecked")
    boolean run(String[] args) {
        // the cli has commands and args is empty
        if (!spec.commands().isEmpty() && args.length == 0) {
            usage();
            return false;
        }

        int cmdPos = IntStream.range(0, args.length)
            .filter(pos -> commandMap.containsKey(args[pos]))
            .findFirst()
            .orElse(args.length);

        String[] flagArgs = Arrays.copyOfRange(args, 0, cmdPos);

        // if this cli is a sub-command group, the flag args should be empty
        if (spec.isCommandGroup() && flagArgs.length > 0) {
            usage();
            return false;
        }

        // parse flags
        if (flagArgs.length != 0) {
            // skip parsing flags
            try {
                commander.parse(flagArgs);
            } catch (Exception e) {
                usage(e.getMessage());
                return false;
            }
        }

        // print help messages
        if (spec.flags().help) {
            usage();
            return false;
        }

        // found a sub command
        if (cmdPos == args.length) {
            return runFunc.apply(spec.flags());
        } else {
            String cmd = args[cmdPos];
            Command command = commandMap.get(cmd);
            String[] subCmdArgs = Arrays.copyOfRange(
                args, cmdPos + 1, args.length);

            try {
                return command.apply(spec.flags(), subCmdArgs);
            } catch (Exception e) {
                e.printStackTrace(spec.console());
                usage(e.getMessage());
                return false;
            }
        }
    }

    public static <CliOptsT extends CliFlags> int runCli(
            CliSpec<CliOptsT> spec, String[] args) {
        Cli<CliOptsT> cli = new Cli<>(spec);
        return cli.run(args) ? 0 : -1;
    }

    public static <CliOptsT extends CliFlags> void printUsage(CliSpec<CliOptsT> spec) {
        Cli<CliOptsT> cli = new Cli<>(spec);
        cli.usage();
    }
}
