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
package org.apache.bookkeeper.stream.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.stream.cli.commands.CmdBase;
import org.apache.bookkeeper.stream.cli.commands.CmdNamespace;
import org.apache.bookkeeper.stream.cli.commands.CmdStream;
import org.apache.bookkeeper.stream.cli.commands.CmdTable;
import org.apache.bookkeeper.stream.proto.common.Endpoint;

/**
 * Bookie Shell.
 */
@Slf4j
public class StreamStorageCli {

    /**
     * Make this command map static. This provides a way to plugin different sub commands.
     */
    private static final Map<String, Class> commandMap;

    static {
        commandMap = new TreeMap<>();

        // build the default command map
        commandMap.put("namespace", CmdNamespace.class);
        commandMap.put("stream", CmdStream.class);
        commandMap.put("table", CmdTable.class);
    }

    static JCommander newJCommander() {
        return new JCommander();
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    public static Object newCommandInstance(Class cls,
                                            StorageClientSettings.Builder settingsBuilder)
            throws Exception {
        return cls.getConstructor(StorageClientSettings.Builder.class)
            .newInstance(settingsBuilder);
    }

    public static void registerSubcommand(String commandName, Class commandClass) {
        synchronized (commandMap) {
            commandMap.put(commandName, commandClass);
        }
    }

    public static void unregisterSubcommand(String commandName) {
        synchronized (commandMap) {
            commandMap.remove(commandName);
        }
    }

    @Getter(AccessLevel.PACKAGE)
    static class ShellArguments {

        @Parameter(names = { "-s", "--server" }, description = "A storage server address")
        private String endpoint = null;

        @Parameter(names = { "-n", "--namespace" }, description = "Namespace")
        private String namespace = "default";

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;

    }

    @Getter(value = AccessLevel.PACKAGE)
    private final ShellArguments shellArgs;
    @Getter(value = AccessLevel.PACKAGE)
    private final JCommander commander;
    private final StorageClientSettings.Builder settingsBuilder;

    StreamStorageCli() throws Exception {
        this.shellArgs = new ShellArguments();
        this.commander = newJCommander();
        this.commander.setProgramName("stream-storage-cli");
        this.commander.addObject(shellArgs);

        this.settingsBuilder = StorageClientSettings.newBuilder()
            .clientName("stream-storage-cli")
            .usePlaintext(true);
    }

    void setupShell() {
        for (Entry<String, Class> entry : commandMap.entrySet()) {
            try {
                Object obj = newCommandInstance(entry.getValue(), settingsBuilder);
                log.info("Setup command {}", entry.getValue());
                this.commander.addCommand(
                    entry.getKey(),
                    obj);
            } catch (Exception e) {
                System.err.println("Fail to load sub command '" + entry.getKey() + "' : " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    boolean run(String[] args) {
        setupShell();
        if (args.length == 0) {
            commander.usage();
            return false;
        }

        int cmdPos;
        for (cmdPos = 0; cmdPos < args.length; cmdPos++) {
            if (commandMap.containsKey(args[cmdPos])) {
                break;
            }
        }

        try {
            commander.parse(Arrays.copyOfRange(args, 0, Math.min(cmdPos, args.length)));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            commander.usage();
            return false;
        }

        if (shellArgs.help) {
            commander.usage();
            return false;
        }

        if (null == shellArgs.endpoint) {
            System.err.println("No endpoint is provided");
            commander.usage();
            return false;
        }

        Endpoint endpoint = NetUtils.parseEndpoint(shellArgs.endpoint);
        settingsBuilder.addEndpoints(endpoint);

        log.info("connecting to storage service = {}", endpoint);

        if (cmdPos == args.length) {
            commander.usage();
            return false;
        } else {
            String cmd = args[cmdPos];
            JCommander subCmd = commander.getCommands().get(cmd);
            CmdBase subCmdObj = (CmdBase) subCmd.getObjects().get(0);
            String[] subCmdArgs = Arrays.copyOfRange(args, cmdPos + 1, args.length);

            return subCmdObj.run(shellArgs.namespace, subCmdArgs);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamStorageCli shell = new StreamStorageCli();

        if (shell.run(args)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

}
