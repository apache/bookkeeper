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
package org.apache.distributedlog.impl.logsegment;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.bk.DynamicQuorumConfigProvider;
import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.bk.LedgerAllocatorDelegator;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.bk.QuorumConfigProvider;
import org.apache.distributedlog.bk.SimpleLedgerAllocator;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryReader;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.logsegment.LogSegmentRandomAccessEntryReader;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.Allocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * BookKeeper Based Entry Store.
 */
public class BKLogSegmentEntryStore implements
        LogSegmentEntryStore,
        AsyncCallback.OpenCallback,
        AsyncCallback.DeleteCallback {

    private static final Logger logger = LoggerFactory.getLogger(BKLogSegmentEntryReader.class);

    private static class OpenReaderRequest {

        private final LogSegmentMetadata segment;
        private final long startEntryId;
        private final CompletableFuture<LogSegmentEntryReader> openPromise;

        OpenReaderRequest(LogSegmentMetadata segment,
                          long startEntryId) {
            this.segment = segment;
            this.startEntryId = startEntryId;
            this.openPromise = new CompletableFuture<LogSegmentEntryReader>();
        }

    }

    private static class DeleteLogSegmentRequest {

        private final LogSegmentMetadata segment;
        private final CompletableFuture<LogSegmentMetadata> deletePromise;

        DeleteLogSegmentRequest(LogSegmentMetadata segment) {
            this.segment = segment;
            this.deletePromise = new CompletableFuture<LogSegmentMetadata>();
        }

    }

    private final byte[] passwd;
    private final ZooKeeperClient zkc;
    private final BookKeeperClient bkc;
    private final OrderedScheduler scheduler;
    private final DistributedLogConfiguration conf;
    private final DynamicDistributedLogConfiguration dynConf;
    private final StatsLogger statsLogger;
    private final AsyncFailureInjector failureInjector;
    // ledger allocator
    private final LedgerAllocator allocator;

    public BKLogSegmentEntryStore(DistributedLogConfiguration conf,
                                  DynamicDistributedLogConfiguration dynConf,
                                  ZooKeeperClient zkc,
                                  BookKeeperClient bkc,
                                  OrderedScheduler scheduler,
                                  LedgerAllocator allocator,
                                  StatsLogger statsLogger,
                                  AsyncFailureInjector failureInjector) {
        this.conf = conf;
        this.dynConf = dynConf;
        this.zkc = zkc;
        this.bkc = bkc;
        this.passwd = conf.getBKDigestPW().getBytes(UTF_8);
        this.scheduler = scheduler;
        this.allocator = allocator;
        this.statsLogger = statsLogger;
        this.failureInjector = failureInjector;
    }

    @Override
    public CompletableFuture<LogSegmentMetadata> deleteLogSegment(LogSegmentMetadata segment) {
        DeleteLogSegmentRequest request = new DeleteLogSegmentRequest(segment);
        BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        bk.asyncDeleteLedger(segment.getLogSegmentId(), this, request);
        return request.deletePromise;
    }

    @Override
    public void deleteComplete(int rc, Object ctx) {
        DeleteLogSegmentRequest deleteRequest = (DeleteLogSegmentRequest) ctx;
        if (Code.NoSuchLedgerExistsOnMetadataServerException == rc) {
            logger.warn("No ledger {} found to delete for {}.",
                    deleteRequest.segment.getLogSegmentId(), deleteRequest.segment);
        } else if (BKException.Code.OK != rc) {
            logger.error("Couldn't delete ledger {} from bookkeeper for {} : {}",
                deleteRequest.segment.getLogSegmentId(), deleteRequest.segment, BKException.getMessage(rc));
            FutureUtils.completeExceptionally(deleteRequest.deletePromise,
                    new BKTransmitException("Couldn't delete log segment " + deleteRequest.segment, rc));
            return;
        }
        FutureUtils.complete(deleteRequest.deletePromise, deleteRequest.segment);
    }

    //
    // Writers
    //

    LedgerAllocator createLedgerAllocator(LogMetadataForWriter logMetadata,
                                          DynamicDistributedLogConfiguration dynConf,
                                          LedgerMetadata ledgerMetadata)
            throws IOException {
        LedgerAllocator ledgerAllocatorDelegator;
        if (null == allocator || !dynConf.getEnableLedgerAllocatorPool()) {
            QuorumConfigProvider quorumConfigProvider =
                    new DynamicQuorumConfigProvider(dynConf);
            LedgerAllocator allocator = new SimpleLedgerAllocator(
                    logMetadata.getAllocationPath(),
                    logMetadata.getAllocationData(),
                    quorumConfigProvider,
                    zkc,
                    bkc,
                    ledgerMetadata);
            ledgerAllocatorDelegator = new LedgerAllocatorDelegator(allocator, true);
        } else {
            ledgerAllocatorDelegator = allocator;
        }
        return ledgerAllocatorDelegator;
    }

    @Override
    public Allocator<LogSegmentEntryWriter, Object> newLogSegmentAllocator(
            LogMetadataForWriter logMetadata,
            DynamicDistributedLogConfiguration dynConf) throws IOException {
        return newLogSegmentAllocator(logMetadata, dynConf, null);
    }

    @Override
    public Allocator<LogSegmentEntryWriter, Object> newLogSegmentAllocator(
            LogMetadataForWriter logMetadata,
            DynamicDistributedLogConfiguration dynConf,
            LedgerMetadata ledgerMetadata) throws IOException {
        // Build the ledger allocator
        LedgerAllocator allocator = createLedgerAllocator(logMetadata, dynConf, ledgerMetadata);
        return new BKLogSegmentAllocator(allocator);
    }

    //
    // Readers
    //

    @Override
    public CompletableFuture<LogSegmentEntryReader> openReader(LogSegmentMetadata segment,
                                                    long startEntryId) {
        BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        OpenReaderRequest request = new OpenReaderRequest(segment, startEntryId);
        if (segment.isInProgress()) {
            bk.asyncOpenLedgerNoRecovery(
                    segment.getLogSegmentId(),
                    BookKeeper.DigestType.CRC32,
                    passwd,
                    this,
                    request);
        } else {
            bk.asyncOpenLedger(
                    segment.getLogSegmentId(),
                    BookKeeper.DigestType.CRC32,
                    passwd,
                    this,
                    request);
        }
        return request.openPromise;
    }

    @Override
    public void openComplete(int rc, LedgerHandle lh, Object ctx) {
        OpenReaderRequest request = (OpenReaderRequest) ctx;
        if (BKException.Code.OK != rc) {
            FutureUtils.completeExceptionally(
                    request.openPromise,
                    new BKTransmitException("Failed to open ledger handle for log segment " + request.segment, rc));
            return;
        }
        // successfully open a ledger
        try {
            LogSegmentEntryReader reader = new BKLogSegmentEntryReader(
                    request.segment,
                    lh,
                    request.startEntryId,
                    bkc.get(),
                    scheduler,
                    conf,
                    statsLogger,
                    failureInjector);
            FutureUtils.complete(request.openPromise, reader);
        } catch (IOException e) {
            FutureUtils.completeExceptionally(request.openPromise, e);
        }

    }

    @Override
    public CompletableFuture<LogSegmentRandomAccessEntryReader> openRandomAccessReader(final LogSegmentMetadata segment,
                                                                            final boolean fence) {
        final BookKeeper bk;
        try {
            bk = this.bkc.get();
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        final CompletableFuture<LogSegmentRandomAccessEntryReader> openPromise =
                new CompletableFuture<LogSegmentRandomAccessEntryReader>();
        AsyncCallback.OpenCallback openCallback = new AsyncCallback.OpenCallback() {
            @Override
            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                if (BKException.Code.OK != rc) {
                    FutureUtils.completeExceptionally(
                            openPromise,
                            new BKTransmitException("Failed to open ledger handle for log segment " + segment, rc));
                    return;
                }
                LogSegmentRandomAccessEntryReader reader = new BKLogSegmentRandomAccessEntryReader(
                        segment,
                        lh,
                        conf);
                FutureUtils.complete(openPromise, reader);
            }
        };
        if (segment.isInProgress() && !fence) {
            bk.asyncOpenLedgerNoRecovery(
                    segment.getLogSegmentId(),
                    BookKeeper.DigestType.CRC32,
                    passwd,
                    openCallback,
                    null);
        } else {
            bk.asyncOpenLedger(
                    segment.getLogSegmentId(),
                    BookKeeper.DigestType.CRC32,
                    passwd,
                    openCallback,
                    null);
        }
        return openPromise;
    }
}
