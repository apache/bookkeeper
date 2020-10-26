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
import org.apache.bookkeeper.tools.cli.commands.bookie.ConvertToDBStorageCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ConvertToInterleavedStorageCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.FlipBookieIdCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.FormatCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.InitCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LastMarkCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LedgerCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListFilesOnDiscCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListLedgersCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LocalConsistencyCheckCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadJournalCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLedgerCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLogCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLogMetadataCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.RebuildDBLedgerLocationsIndexCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.RegenerateInterleavedStorageIndexFileCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.SanityTestCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.EndpointInfoCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliCommandGroup;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Commands that operates a single bookie.
 */
public class BookieCommandGroup extends CliCommandGroup<BKFlags> {

    private static final String NAME = "bookie";
    private static final String DESC = "Commands on operating a single bookie";

    private static final CliSpec<BKFlags> spec = CliSpec.<BKFlags>newBuilder()
        .withName(NAME)
        .withDescription(DESC)
        .withParent(BKCtl.NAME)
        .withCategory(CATEGORY_INFRA_SERVICE)
        .addCommand(new LastMarkCommand())
        .addCommand(new InitCommand())
        .addCommand(new FormatCommand())
        .addCommand(new EndpointInfoCommand())
        .addCommand(new SanityTestCommand())
        .addCommand(new LedgerCommand())
        .addCommand(new ListFilesOnDiscCommand())
        .addCommand(new ConvertToDBStorageCommand())
        .addCommand(new ListLedgersCommand())
        .addCommand(new ConvertToInterleavedStorageCommand())
        .addCommand(new ReadJournalCommand())
        .addCommand(new RebuildDBLedgerLocationsIndexCommand())
        .addCommand(new ReadLedgerCommand())
        .addCommand(new ReadLogCommand())
        .addCommand(new ReadLogMetadataCommand())
        .addCommand(new LocalConsistencyCheckCommand())
        .addCommand(new FlipBookieIdCommand())
        .addCommand(new RegenerateInterleavedStorageIndexFileCommand())
        .build();

    public BookieCommandGroup() {
        super(spec);
    }
}
