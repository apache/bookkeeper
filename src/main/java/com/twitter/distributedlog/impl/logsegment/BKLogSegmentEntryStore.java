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
package com.twitter.distributedlog.impl.logsegment;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.injector.AsyncFailureInjector;
import com.twitter.distributedlog.logsegment.LogSegmentEntryReader;
import com.twitter.distributedlog.logsegment.LogSegmentEntryStore;
import com.twitter.distributedlog.logsegment.LogSegmentEntryWriter;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.StatsLogger;

import static com.google.common.base.Charsets.UTF_8;

/**
 * BookKeeper Based Entry Store
 */
public class BKLogSegmentEntryStore implements LogSegmentEntryStore, AsyncCallback.OpenCallback {

    private static class OpenReaderRequest {

        private final LogSegmentMetadata segment;
        private final long startEntryId;
        private final Promise<LogSegmentEntryReader> openPromise;

        OpenReaderRequest(LogSegmentMetadata segment,
                          long startEntryId) {
            this.segment = segment;
            this.startEntryId = startEntryId;
            this.openPromise = new Promise<LogSegmentEntryReader>();
        }

    }

    private final byte[] passwd;
    private final BookKeeper bk;
    private final OrderedScheduler scheduler;
    private final DistributedLogConfiguration conf;
    private final StatsLogger statsLogger;
    private final AsyncFailureInjector failureInjector;

    public BKLogSegmentEntryStore(DistributedLogConfiguration conf,
                                  BookKeeper bk,
                                  OrderedScheduler scheduler,
                                  StatsLogger statsLogger,
                                  AsyncFailureInjector failureInjector) {
        this.conf = conf;
        this.bk = bk;
        this.passwd = conf.getBKDigestPW().getBytes(UTF_8);
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        this.failureInjector = failureInjector;
    }

    @Override
    public Future<LogSegmentEntryWriter> openWriter(LogSegmentMetadata segment) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public Future<LogSegmentEntryReader> openReader(LogSegmentMetadata segment,
                                                    long startEntryId) {
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
            FutureUtils.setException(
                    request.openPromise,
                    new BKTransmitException("Failed to open ledger handle for log segment " + request.segment, rc));
            return;
        }
        // successfully open a ledger
        LogSegmentEntryReader reader = new BKLogSegmentEntryReader(
                request.segment,
                lh,
                request.startEntryId,
                bk,
                scheduler,
                conf,
                statsLogger,
                failureInjector);
        FutureUtils.setValue(request.openPromise, reader);
    }
}
