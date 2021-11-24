/**
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
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;

import io.netty.buffer.ByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.configuration.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralizes the creation of injected resources.
 */
public class BookieResources {
    private static final Logger log = LoggerFactory.getLogger(BookieResources.class);

    /**
     * Instantiate the metadata driver for the Bookie.
     */
    public static MetadataBookieDriver createMetadataDriver(ServerConfiguration conf,
                                                            StatsLogger statsLogger) throws BookieException {
        try {
            String metadataServiceUriStr = conf.getMetadataServiceUri();
            if (null == metadataServiceUriStr) {
                throw new BookieException.MetadataStoreException("Metadata URI must not be null");
            }

            MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(
                URI.create(metadataServiceUriStr));
            driver.initialize(conf, statsLogger.scope(BOOKIE_SCOPE));
            return driver;
        } catch (MetadataException me) {
            throw new BookieException.MetadataStoreException("Failed to initialize metadata bookie driver", me);
        } catch (ConfigurationException e) {
            throw new BookieException.BookieIllegalOpException(e);
        }
    }

    public static ByteBufAllocatorWithOomHandler createAllocator(ServerConfiguration conf) {
        return ByteBufAllocatorBuilder.create()
            .poolingPolicy(conf.getAllocatorPoolingPolicy())
            .poolingConcurrency(conf.getAllocatorPoolingConcurrency())
            .outOfMemoryPolicy(conf.getAllocatorOutOfMemoryPolicy())
            .leakDetectionPolicy(conf.getAllocatorLeakDetectionPolicy())
            .build();
    }

    public static DiskChecker createDiskChecker(ServerConfiguration conf) {
        return new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
    }

    public static LedgerDirsManager createLedgerDirsManager(ServerConfiguration conf, DiskChecker diskChecker,
                                                            StatsLogger statsLogger) throws IOException {
        return new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker, statsLogger);
    }

    public static LedgerDirsManager createIndexDirsManager(ServerConfiguration conf, DiskChecker diskChecker,
                                                           StatsLogger statsLogger, LedgerDirsManager fallback)
            throws IOException {
        File[] idxDirs = conf.getIndexDirs();
        if (null == idxDirs) {
            return fallback;
        } else {
            return new LedgerDirsManager(conf, idxDirs, diskChecker, statsLogger);
        }
    }

    public static LedgerStorage createLedgerStorage(ServerConfiguration conf,
                                                    LedgerManager ledgerManager,
                                                    LedgerDirsManager ledgerDirsManager,
                                                    LedgerDirsManager indexDirsManager,
                                                    StatsLogger statsLogger,
                                                    ByteBufAllocator allocator) throws IOException {
        // Instantiate the ledger storage implementation
        String ledgerStorageClass = conf.getLedgerStorageClass();
        log.info("Using ledger storage: {}", ledgerStorageClass);
        LedgerStorage storage = LedgerStorageFactory.createLedgerStorage(ledgerStorageClass);

        storage.initialize(conf, ledgerManager, ledgerDirsManager, indexDirsManager, statsLogger, allocator);
        storage.setCheckpointSource(CheckpointSource.DEFAULT);
        return storage;
    }


}
