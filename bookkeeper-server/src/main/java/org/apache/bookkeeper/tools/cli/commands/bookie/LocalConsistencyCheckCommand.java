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

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.List;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Command to check local storage for inconsistencies.
 */
@CustomLog
public class LocalConsistencyCheckCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "localconsistencycheck";
    private static final String DESC = "Validate Ledger Storage internal metadata";

    public LocalConsistencyCheckCommand() {
        super(CliSpec.newBuilder()
                     .withName(NAME)
                     .withDescription(DESC)
                     .withFlags(new CliFlags())
                     .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        try {
            return check(conf);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean check(ServerConfiguration conf) throws IOException {
        log.info("=== Performing local consistency check ===");
        ServerConfiguration serverConfiguration = new ServerConfiguration(conf);
        LedgerStorage ledgerStorage = BookieImpl.mountLedgerStorageOffline(serverConfiguration, null);
        List<LedgerStorage.DetectedInconsistency> errors = ledgerStorage.localConsistencyCheck(
            java.util.Optional.empty());
        if (errors.size() > 0) {
            log.info("=== Check returned errors: ===");
            for (LedgerStorage.DetectedInconsistency error : errors) {
                log.error()
                        .attr("ledgerId", error.getLedgerId())
                        .attr("entryId", error.getEntryId())
                        .exception(error.getException())
                        .log("Inconsistency detected for ledger entry");
            }
            return false;
        } else {
            log.info("=== Check passed ===");
            return true;
        }
    }
}
