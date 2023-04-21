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

import com.google.common.collect.Sets;
import java.io.PrintStream;
import java.util.Set;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A spec to build CLI.
 */
@ToString
@EqualsAndHashCode
public class CliSpec<CliFlagsT extends CliFlags> {

    /**
     * Create a new builder to build the cli spec.
     *
     * @return a new builder to build the cli spec.
     */
    public static <T extends CliFlags> Builder<T> newBuilder() {
        return new Builder<>();
    }

    /**
     * Create a new builder to build the cli spec from an existing <tt>spec</tt>.
     *
     * @param spec cli spec
     * @return a new builder to build the cli spec from an existing <tt>spec</tt>.
     */
    public static <T extends CliFlags> Builder<T> newBuilder(CliSpec<T> spec) {
        return new Builder<>(spec);
    }

    /**
     * Builder to build a cli spec.
     */
    public static class Builder<CliFlagsT extends CliFlags> {

        private String name = "unknown";
        private String parent = "";
        private String usage = "";
        private CliFlagsT flags = null;
        private String description = "unset";
        private final Set<Command> commands = Sets.newHashSet();
        private String tailer = "";
        private Function<CliFlagsT, Boolean> runFunc = null;
        private PrintStream console = System.out;
        private boolean isCommandGroup = false;
        private String argumentsUsage = "";
        private String category = "";

        private Builder() {}

        private Builder(CliSpec<CliFlagsT> spec) {
            this.category = spec.category;
            this.name = spec.name;
            this.parent = spec.parent;
            this.usage = spec.usage;
            this.argumentsUsage = spec.argumentsUsage;
            this.flags = spec.flags;
            this.description = spec.description;
            this.commands.clear();
            this.commands.addAll(spec.commands);
            this.tailer = spec.tailer;
            this.runFunc = spec.runFunc;
            this.console = spec.console;
            this.isCommandGroup = spec.isCommandGroup;
        }

        public Builder<CliFlagsT> withCategory(String category) {
            this.category = category;
            return this;
        }

        public Builder<CliFlagsT> withName(String name) {
            this.name = name;
            return this;
        }

        public Builder<CliFlagsT> withParent(String parent) {
            this.parent = parent;
            return this;
        }

        public Builder<CliFlagsT> withUsage(String usage) {
            this.usage = usage;
            return this;
        }

        public Builder<CliFlagsT> withArgumentsUsage(String usage) {
            this.argumentsUsage = usage;
            return this;
        }

        public Builder<CliFlagsT> withFlags(CliFlagsT flags) {
            this.flags = flags;
            return this;
        }

        public Builder<CliFlagsT> withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder<CliFlagsT> addCommand(Command command) {
            this.commands.add(command);
            return this;
        }

        public Builder<CliFlagsT> withTailer(String tailer) {
            this.tailer = tailer;
            return this;
        }

        public Builder<CliFlagsT> withRunFunc(Function<CliFlagsT, Boolean> func) {
            this.runFunc = func;
            return this;
        }

        public Builder<CliFlagsT> withConsole(PrintStream console) {
            this.console = console;
            return this;
        }

        public Builder<CliFlagsT> setCommandGroup(boolean enabled) {
            this.isCommandGroup = enabled;
            return this;
        }

        public CliSpec<CliFlagsT> build() {
            return new CliSpec<>(
                category,
                name,
                parent,
                usage,
                argumentsUsage,
                flags,
                description,
                commands,
                tailer,
                runFunc,
                console,
                isCommandGroup
            );
        }

    }

    private final String category;
    private final String name;
    private final String parent;
    private final String usage;
    private final String argumentsUsage;
    private final CliFlagsT flags;
    private final String description;
    private final Set<Command> commands;
    private final String tailer;
    private final Function<CliFlagsT, Boolean> runFunc;
    private final PrintStream console;
    // whether the cli spec is for a command group.
    private final boolean isCommandGroup;

    private CliSpec(String category,
                    String name,
                    String parent,
                    String usage,
                    String argumentsUsage,
                    CliFlagsT flags,
                    String description,
                    Set<Command> commands,
                    String tailer,
                    Function<CliFlagsT, Boolean> runFunc,
                    PrintStream console,
                    boolean isCommandGroup) {
        this.category = category;
        this.name = name;
        this.parent = parent;
        this.usage = usage;
        this.flags = flags;
        this.argumentsUsage = argumentsUsage;
        this.description = description;
        this.commands = commands;
        this.tailer = tailer;
        this.runFunc = runFunc;
        this.console = console;
        this.isCommandGroup = isCommandGroup;
    }

    public String category() {
        return category;
    }

    public String name() {
        return name;
    }

    public String parent() {
        return parent;
    }

    public String usage() {
        return usage;
    }

    public String argumentsUsage() {
        return argumentsUsage;
    }

    public CliFlagsT flags() {
        return flags;
    }

    public String description() {
        return description;
    }

    public Set<Command> commands() {
        return commands;
    }

    public String tailer() {
        return tailer;
    }

    public Function<CliFlagsT, Boolean> runFunc() {
        return runFunc;
    }

    public PrintStream console() {
        return console;
    }

    public boolean isCommandGroup() {
        return isCommandGroup;
    }

}
