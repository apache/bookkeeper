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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to determine if the prior shutdown was unclean or not. It does so
 * by adding a file to each ledger directory after successful start-up
 * and removing the file on graceful shutdown.
 * Any abrupt termination will cause one or more of these files to not be cleared
 * and so on the subsequent boot-up, the presence of any of these files will
 * indicate an unclean shutdown.
 */
public class UncleanShutdownDetectionImpl implements UncleanShutdownDetection {
    private static final Logger LOG = LoggerFactory.getLogger(UncleanShutdownDetectionImpl.class);
    private final LedgerDirsManager ledgerDirsManager;
    static final String DirtyFileName = "DIRTY";

    public UncleanShutdownDetectionImpl(LedgerDirsManager ledgerDirsManager) {
        this.ledgerDirsManager = ledgerDirsManager;
    }

    @Override
    public void registerStartUp() throws IOException {
        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            try {
                File dirtyFile = new File(ledgerDir, DirtyFileName);
                dirtyFile.createNewFile();
                LOG.info("Created dirty file in ledger dir: {}", ledgerDir.getAbsolutePath());
            } catch (IOException e) {
                LOG.error("Unable to register start-up (so an unclean shutdown cannot"
                        + " be detected). Dirty file of ledger dir {} could not be created.",
                        ledgerDir.getAbsolutePath(), e);
                throw e;
            }
        }
    }

    @Override
    public void registerCleanShutdown() {
        for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
            try {
                File dirtyFile = new File(ledgerDir, DirtyFileName);
                dirtyFile.delete();
            } catch (Throwable t) {
                LOG.error("Unable to register a clean shutdown, dirty file of "
                        + " ledger dir {} could not be deleted",
                        ledgerDir.getAbsolutePath(), t);
            }
        }
    }

    @Override
    public boolean lastShutdownWasUnclean() {
        try {
            for (File ledgerDir : ledgerDirsManager.getAllLedgerDirs()) {
                File dirtyFile = new File(ledgerDir, DirtyFileName);
                if (dirtyFile.exists()) {
                    return true;
                }
            }
        } catch (Throwable t) {
            LOG.error("Unable to determine if last shutdown was unclean (defaults to unclean)", t);
            return true;
        }

        return false;
    }
}
