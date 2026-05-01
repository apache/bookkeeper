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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.CustomLog;

/**
 * Used to determine if the prior shutdown was unclean or not. It does so
 * by adding a file to each ledger directory after successful start-up
 * and removing the file on graceful shutdown.
 * Any abrupt termination will cause one or more of these files to not be cleared
 * and so on the subsequent boot-up, the presence of any of these files will
 * indicate an unclean shutdown.
 */
@CustomLog
public class UncleanShutdownDetectionImpl implements UncleanShutdownDetection {
    private final LedgerDirsManager ledgerDirsManager;
    static final String DIRTY_FILENAME = "DIRTY";

    public UncleanShutdownDetectionImpl(LedgerDirsManager ledgerDirsManager) {
        this.ledgerDirsManager = ledgerDirsManager;
    }

    @Override
    public void registerStartUp() throws IOException {
        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            try {
                File dirtyFile = new File(ledgerDir, DIRTY_FILENAME);
                if (dirtyFile.createNewFile()) {
                    log.info().attr("ledgerDir", ledgerDir.getAbsolutePath()).log("Created dirty file in ledger dir");
                } else {
                    log.info().attr("ledgerDir", ledgerDir.getAbsolutePath())
                        .log("Dirty file already exists in ledger dir");
                }

            } catch (IOException e) {
                log.error()
                        .exception(e)
                        .attr("ledgerDir", ledgerDir.getAbsolutePath())
                    .log("Unable to register start-up (so an unclean shutdown cannot"
                        + " be detected). Dirty file of ledger dir could not be created.");
                throw e;
            }
        }
    }

    @Override
    public void registerCleanShutdown() {
        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            try {
                File dirtyFile = new File(ledgerDir, DIRTY_FILENAME);
                if (dirtyFile.exists()) {
                    boolean deleted = dirtyFile.delete();

                    if (!deleted) {
                        log.error().attr("ledgerDir", ledgerDir.getAbsolutePath())
                            .log("Unable to register a clean shutdown. The dirty file of"
                                + " ledger dir could not be deleted.");
                    }
                } else {
                    log.error().attr("ledgerDir", ledgerDir.getAbsolutePath())
                        .log("Unable to register a clean shutdown. The dirty file of ledger dir does not exist.");
                }
            } catch (Throwable t) {
                log.error()
                        .exception(t)
                        .attr("ledgerDir", ledgerDir.getAbsolutePath())
                    .log("Unable to register a clean shutdown. An error occurred while deleting"
                        + " the dirty file of ledger dir.");
            }
        }
    }

    @Override
    public boolean lastShutdownWasUnclean() {
        boolean unclean = false;
        List<String> dirtyFiles = new ArrayList<>();
        try {
            for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
                File dirtyFile = new File(ledgerDir, DIRTY_FILENAME);
                if (dirtyFile.exists()) {
                    dirtyFiles.add(dirtyFile.getAbsolutePath());
                    unclean = true;
                }
            }
        } catch (Throwable t) {
            log.error().exception(t).log("Unable to determine if last shutdown was unclean (defaults to unclean)");
            unclean = true;
        }

        if (!dirtyFiles.isEmpty()) {
            log.info().attr("dirtyFiles", String.join(",", dirtyFiles))
                .log("Dirty files exist on boot-up indicating an unclean shutdown");
        }

        return unclean;
    }
}
