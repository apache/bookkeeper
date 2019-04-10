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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import java.io.IOException;
import org.apache.bookkeeper.bookie.storage.ldb.LocationsIndexRebuildOp;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to rebuild DBLedgerStorage locations index.
 */
public class RebuildDBLedgerLocationsIndexCommand extends BookieCommand<CliFlags> {

    static final Logger LOG = LoggerFactory.getLogger(RebuildDBLedgerLocationsIndexCommand.class);

    private static final String NAME = "rebuild-db-ledger-locations-index";
    private static final String DESC = "Rbuild DBLedgerStorage locations index by scanning the entry logs";

    public RebuildDBLedgerLocationsIndexCommand() {
        super(CliSpec.newBuilder().withName(NAME).withDescription(DESC).withFlags(new CliFlags()).build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        LOG.info("=== Rebuilding bookie index ===");
        ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        try {
            new LocationsIndexRebuildOp(serverConfiguration).initiate();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("-- Done rebuilding bookie index --");
        return true;
    }
}
