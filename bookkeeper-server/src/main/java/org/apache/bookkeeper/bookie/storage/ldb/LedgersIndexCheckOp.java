/*
 *
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
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Scan the ledgers index to make sure it is readable.
 */
@CustomLog
public class LedgersIndexCheckOp {

    private final ServerConfiguration conf;
    private final boolean verbose;
    private static final String LedgersSubPath = "ledgers";

    public LedgersIndexCheckOp(ServerConfiguration conf, boolean verbose) {
        this.conf = conf;
        this.verbose = verbose;
    }

    public boolean initiate() throws IOException {
        File[] indexDirs = conf.getIndexDirs();
        if (indexDirs == null) {
            indexDirs = conf.getLedgerDirs();
        }
        if (indexDirs.length != conf.getLedgerDirs().length) {
            throw new IOException("ledger and index dirs size not matched");
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < conf.getLedgerDirs().length; i++) {
            File indexDir = indexDirs[i];

            String iBasePath = BookieImpl.getCurrentDirectory(indexDir).toString();
            Path indexCurrentPath = FileSystems.getDefault().getPath(iBasePath, LedgersSubPath);

            log.info().attr("indexPath", indexCurrentPath).log("Loading ledgers index");
            log.info("Starting index scan");

            try {
                KeyValueStorage index = new KeyValueStorageRocksDB(iBasePath, LedgersSubPath,
                        DbConfigType.Default, conf, true);
                // Read all ledgers from db
                KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> iterator = index.iterator();
                int ctr = 0;
                try {
                    while (iterator.hasNext()) {
                        ctr++;
                        Map.Entry<byte[], byte[]> entry = iterator.next();
                        long ledgerId = ArrayUtil.getLong(entry.getKey(), 0);
                        DbLedgerStorageDataFormats.LedgerData ledgerData =
                                DbLedgerStorageDataFormats.LedgerData.parseFrom(entry.getValue());
                        if (verbose) {
                            log.info()
                                    .attr("scanned", ctr)
                                    .attr("ledgerId", ledgerId)
                                    .attr("exists", (ledgerData.hasExists() ? ledgerData.getExists() : "-"))
                                    .attr("isFenced", (ledgerData.hasFenced() ? ledgerData.getFenced() : "-"))
                                    .attr("masterKey", (ledgerData.hasMasterKey()
                                            ? Base64.getEncoder()
                                            .encodeToString(ledgerData.getMasterKey().toByteArray())
                                            : "-"))
                                    .attr("explicitLAC", (ledgerData.hasExplicitLac()
                                            ? ledgerData.getExplicitLac() : "-"))
                                    .log("Scanned ledger");
                        } else if (ctr % 100 == 0) {
                            log.info().attr("count", ctr).log("Scanned ledgers");
                        }
                    }
                } finally {
                    iterator.close();
                }
                log.info().attr("count", ctr).log("Scanned ledgers");
            } catch (Throwable t) {
                log.error().exception(t).log("Index scan has failed with error");
                return false;
            }
        }
        log.info().attr("totalTime", DurationFormatUtils.formatDurationHMS(
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)))
                .log("Index scan has completed successfully");
        return true;
    }
}
