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
 * A command that runs a CLI spec. it is typically used for nested sub commands.
 */
public class CliCommand<GlobalFlagsT extends CliFlags, CommandFlagsT extends CliFlags>
        implements Command<GlobalFlagsT> {

    private final String name;
    private String parent;
    protected CliSpec<CommandFlagsT> spec;

    public CliCommand(CliSpec<CommandFlagsT> spec) {
        this.spec = spec;
        this.name = spec.name();
        this.parent = spec.parent();
    }

    public void setParent(String parent) {
        this.parent = parent;
        this.spec = CliSpec.newBuilder(spec)
            .withParent(parent)
            .build();
    }

    @Override
    public String category() {
        return spec.category();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String path() {
        return parent + " " + name;
    }

    @Override
    public String description() {
        return spec.description();
    }

    @Override
    public Boolean apply(GlobalFlagsT globalFlags, String[] args) {
        int res = Cli.runCli(spec, args);
        return res == 0;
    }

    @Override
    public void usage() {
        // run with "empty args", which will print the usage for this command group.
        Cli.printUsage(spec);
    }
}
