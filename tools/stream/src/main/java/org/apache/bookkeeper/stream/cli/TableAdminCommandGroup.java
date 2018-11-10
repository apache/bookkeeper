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

import static org.apache.bookkeeper.tools.common.BKCommandCategories.CATEGORY_TABLE_SERVICE;

import org.apache.bookkeeper.stream.cli.commands.table.CreateTableCommand;
import org.apache.bookkeeper.stream.cli.commands.table.DeleteTableCommand;
import org.apache.bookkeeper.stream.cli.commands.table.GetTableCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliCommandGroup;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Commands that admin tables.
 */
public class TableAdminCommandGroup extends CliCommandGroup<BKFlags> {

    private static final String NAME = "tables";
    private static final String DESC = "Commands on operating tables";

    private static final CliSpec<BKFlags> spec = CliSpec.<BKFlags>newBuilder()
        .withName(NAME)
        .withDescription(DESC)
        .withParent("bkctl")
        .withCategory(CATEGORY_TABLE_SERVICE)
        .addCommand(new CreateTableCommand())
        .addCommand(new GetTableCommand())
        .addCommand(new DeleteTableCommand())
        .build();

    public TableAdminCommandGroup() {
        super(spec);
    }
}
