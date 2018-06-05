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

import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;
import org.apache.bookkeeper.tools.framework.commands.TestCommand;

/**
 * A CLI used for testing.
 */
public class TestCli {

    private static final String NAME = "bktest";

    private static final String DESC = "bookkeeper test";

    static class TestFlags extends CliFlags {

        @Parameter(
            names = {
                "-s", "--string-flag"
            },
            description = "string flag")
        public String stringFlag = null;

        @Parameter(
            names = {
                "-i", "--int-flag"
            },
            description = "int flag")
        public int intFlag = 0;

        @Parameter(
            names = {
                "-l", "--list-flags"
            },
            description = "list flag")
        public List<String> listFlag = Lists.newArrayList();

    }

    static class TestNestedFlags extends CliFlags {

        @Parameter(
            names = {
                "-s", "--nested-string-flag"
            },
            description = "string flag")
        public String stringFlag = null;

        @Parameter(
            names = {
                "-i", "--nested-int-flag"
            },
            description = "int flag")
        public int intFlag = 0;

        @Parameter(
            names = {
                "-l", "--nested-list-flags"
            },
            description = "list flag")
        public List<String> listFlag = Lists.newArrayList();

    }

    public static void main(String[] args) {
        Runtime.getRuntime().exit(
            doMain(System.out, args));
    }

    static int doMain(PrintStream console, String[] args) {
        return doMain(console, args, null);
    }

    static int doMain(PrintStream console,
                      String[] args,
                      Function<TestNestedFlags, Boolean> func) {
        String nestedCommandName = "nested";
        String nestedCommandDesc = "bookkeeper test-nested";
        CliSpec<TestNestedFlags> nestedSpec = CliSpec.<TestNestedFlags>newBuilder()
            .withName(nestedCommandName)
            .withParent(NAME)
            .withDescription(nestedCommandDesc)
            .withFlags(new TestNestedFlags())
            .withConsole(console)
            .addCommand(new TestCommand("fish", console))
            .addCommand(new TestCommand("cat", console))
            .withRunFunc(func)
            .build();
        CliCommand<TestFlags, TestNestedFlags> nestedCommand =
            new CliCommand<>(nestedSpec);

        CliSpec<TestFlags> spec = CliSpec.<TestFlags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new TestFlags())
            .withConsole(console)
            .addCommand(new TestCommand("monkey", console))
            .addCommand(new TestCommand("dog", console))
            .addCommand(nestedCommand)
            .build();

        return Cli.runCli(spec, args);
    }



}
