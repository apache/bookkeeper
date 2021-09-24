/**
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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan the ledgers index to make sure it is readable.
 */
public class LedgersIndexCheckOp {
    private static final Logger LOG = LoggerFactory.getLogger(LedgersIndexCheckOp.class);

    private final ServerConfiguration conf;
    private final boolean verbose;
    private static final String LedgersSubPath = "ledgers";

    public LedgersIndexCheckOp(ServerConfiguration conf, boolean verbose) {
        this.conf = conf;
        this.verbose = verbose;
    }

    public boolean initiate() throws IOException {
        String basePath = BookieImpl.getCurrentDirectory(conf.getLedgerDirs()[0]).toString();
        Path currentPath = FileSystems.getDefault().getPath(basePath, LedgersSubPath);

        LOG.info("Loading ledgers index from {}", currentPath);

        long startTime = System.nanoTime();
        LOG.info("Starting index scan");

        try {
            KeyValueStorage index = new KeyValueStorageRocksDB(basePath, LedgersSubPath,
                    DbConfigType.Small, conf, true);
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
                        LOG.info("Scanned: {}, ledger: {}, exists: {}, isFenced: {}, masterKey: {}, explicitLAC: {}",
                                ctr,
                                ledgerId,
                                (ledgerData.hasExists() ? ledgerData.getExists() : "-"),
                                (ledgerData.hasFenced() ? ledgerData.getFenced() : "-"),
                                (ledgerData.hasMasterKey()
                                        ? Base64.getEncoder()
                                            .encodeToString(ledgerData.getMasterKey().toByteArray())
                                        : "-"),
                                (ledgerData.hasExplicitLac() ? ledgerData.getExplicitLac() : "-"));
                    } else if (ctr % 100 == 0) {
                        LOG.info("Scanned {} ledgers", ctr);
                    }
                }
            } finally {
                iterator.close();
            }

            LOG.info("Scanned {} ledgers", ctr);
            LOG.info("Index scan has completed successfully. Total time: {}",
                    DurationFormatUtils.formatDurationHMS(
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)));
        } catch (Throwable t) {
            LOG.error("Index scan has failed with error", t);
            return false;
        }

        return true;
    }
}
