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

import com.google.common.base.Strings;

/**
 * A command group that group commands together. They share same global flags.
 */
public abstract class CliCommandGroup<GlobalFlagsT extends CliFlags>
        extends CliCommand<GlobalFlagsT, GlobalFlagsT> implements CommandGroup<GlobalFlagsT> {

    @SuppressWarnings("unchecked")
    private static <GlobalFlagsT extends CliFlags> CliSpec<GlobalFlagsT> updateSpec(CliSpec<GlobalFlagsT> spec) {
        CliSpec<GlobalFlagsT> newSpec;
        if (Strings.isNullOrEmpty(spec.usage())) {
            newSpec = CliSpec.newBuilder(spec)
                .withUsage(String.format("%s %s [command] [command options]",
                    spec.parent(), spec.name()
                ))
                .withFlags(null)
                .build();
        } else {
            newSpec = CliSpec.newBuilder(spec)
                .withFlags(null)
                .build();
        }

        String path = newSpec.parent() + " " + newSpec.name();

        for (Command<GlobalFlagsT> cmd : newSpec.commands()) {
            if (cmd instanceof CliCommand) {
                CliCommand<GlobalFlagsT, GlobalFlagsT> cliCmd = (CliCommand<GlobalFlagsT, GlobalFlagsT>) cmd;
                cliCmd.setParent(path);
            }
        }

        return newSpec;
    }

    protected CliCommandGroup(CliSpec<GlobalFlagsT> spec) {
        super(updateSpec(spec));
    }

    @Override
    public Boolean apply(GlobalFlagsT globalFlags, String[] args) {
        CliSpec<GlobalFlagsT> newSpec = CliSpec.newBuilder(spec)
            .withFlags(globalFlags)
            .setCommandGroup(true)
            .build();

        return 0 == Cli.runCli(newSpec, args);
    }

    @Override
    public void usage() {
        CliSpec<GlobalFlagsT> newSpec = CliSpec.newBuilder(spec)
            .setCommandGroup(true)
            .build();

        // run with "empty args", which will print the usage for this command group.
        Cli.printUsage(newSpec);
    }
}
