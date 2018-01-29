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
package org.apache.bookkeeper.stream.cli.commands;

import com.beust.jcommander.Parameters;
import org.apache.bookkeeper.clients.config.StorageClientSettings.Builder;
import org.apache.bookkeeper.stream.cli.commands.table.AppendCommand;
import org.apache.bookkeeper.stream.cli.commands.table.GetCommand;
import org.apache.bookkeeper.stream.cli.commands.table.IncrementCommand;
import org.apache.bookkeeper.stream.cli.commands.table.PutCommand;

/**
 * Commands that operates a single bookie.
 */
@Parameters(commandDescription = "Commands on operating tables")
public class CmdTable extends CmdBase {
    public CmdTable(Builder settingsBuilder) {
        super("table", settingsBuilder);
        addSubCommand(new PutCommand());
        addSubCommand(new GetCommand());
        addSubCommand(new IncrementCommand());
        addSubCommand(new AppendCommand());
    }
}
