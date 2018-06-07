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

package org.apache.bookkeeper.tools.framework.commands;

import java.io.PrintStream;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.Command;

/**
 * Test Command.
 */
public class TestCommand implements Command<CliFlags> {

    private final String label;
    private final PrintStream console;

    public TestCommand(String label,
                       PrintStream console) {
        this.label = label;
        this.console = console;
    }

    @Override
    public String name() {
        return label;
    }

    @Override
    public String description() {
        return "Command " + label;
    }

    @Override
    public Boolean apply(CliFlags globalFlags, String[] args) {
        return true;
    }

    @Override
    public void usage() {
        console.println("command " + label);
    }
}
