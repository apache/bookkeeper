/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.tools.cli.commands;

import static org.apache.bookkeeper.tools.common.BKCommandCategories.CATEGORY_INFRA_SERVICE;

import org.apache.bookkeeper.tools.cli.BKCtl;
import org.apache.bookkeeper.tools.cli.commands.bookies.DecommissionCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.InfoCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.InitCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.InstanceIdCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.MetaFormatCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.NukeExistingClusterCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.RecoverCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliCommandGroup;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Commands that interact with a cluster of bookies.
 */
public class BookiesCommandGroup extends CliCommandGroup<BKFlags> {

    private static final String NAME = "bookies";
    private static final String DESC = "Commands on operating a cluster of bookies";

    private static final CliSpec<BKFlags> spec = CliSpec.<BKFlags>newBuilder()
        .withName(NAME)
        .withDescription(DESC)
        .withParent(BKCtl.NAME)
        .withCategory(CATEGORY_INFRA_SERVICE)
        .addCommand(new ListBookiesCommand())
        .addCommand(new InfoCommand())
        .addCommand(new NukeExistingClusterCommand())
        .addCommand(new MetaFormatCommand())
        .addCommand(new DecommissionCommand())
        .addCommand(new InitCommand())
        .addCommand(new RecoverCommand())
        .addCommand(new InstanceIdCommand())
        .build();

    public BookiesCommandGroup() {
        super(spec);
    }
}
