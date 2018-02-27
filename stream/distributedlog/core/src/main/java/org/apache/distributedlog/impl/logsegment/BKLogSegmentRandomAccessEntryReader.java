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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.Entry;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.logsegment.LogSegmentRandomAccessEntryReader;

/**
 * BookKeeper ledger based random access entry reader.
 */
class BKLogSegmentRandomAccessEntryReader implements
        LogSegmentRandomAccessEntryReader,
        ReadCallback {

    private final long lssn;
    private final long startSequenceId;
    private final boolean envelopeEntries;
    private final boolean deserializeRecordSet;
    // state
    private final LogSegmentMetadata metadata;
    private final LedgerHandle lh;
    private CompletableFuture<Void> closePromise = null;

    BKLogSegmentRandomAccessEntryReader(LogSegmentMetadata metadata,
                                        LedgerHandle lh,
                                        DistributedLogConfiguration conf) {
        this.metadata = metadata;
        this.lssn = metadata.getLogSegmentSequenceNumber();
        this.startSequenceId = metadata.getStartSequenceId();
        this.envelopeEntries = metadata.getEnvelopeEntries();
        this.deserializeRecordSet = conf.getDeserializeRecordSetOnReads();
        this.lh = lh;
    }

    @Override
    public long getLastAddConfirmed() {
        return lh.getLastAddConfirmed();
    }

    @Override
    public CompletableFuture<List<Entry.Reader>> readEntries(long startEntryId, long endEntryId) {
        CompletableFuture<List<Entry.Reader>> promise = new CompletableFuture<List<Entry.Reader>>();
        lh.asyncReadEntries(startEntryId, endEntryId, this, promise);
        return promise;
    }

    Entry.Reader processReadEntry(LedgerEntry entry) throws IOException {
        return Entry.newBuilder()
                .setLogSegmentInfo(lssn, startSequenceId)
                .setEntryId(entry.getEntryId())
                .setEnvelopeEntry(envelopeEntries)
                .deserializeRecordSet(deserializeRecordSet)
                .setEntry(entry.getEntryBuffer())
                .buildReader();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
        CompletableFuture<List<Entry.Reader>> promise = (CompletableFuture<List<Entry.Reader>>) ctx;
        if (BKException.Code.OK == rc) {
            List<Entry.Reader> entryList = Lists.newArrayList();
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                try {
                    entryList.add(processReadEntry(entry));
                } catch (IOException ioe) {
                    // release the buffers
                    while (entries.hasMoreElements()) {
                        LedgerEntry le = entries.nextElement();
                        le.getEntryBuffer().release();
                    }
                    FutureUtils.completeExceptionally(promise, ioe);
                    return;
                } finally {
                    entry.getEntryBuffer().release();
                }
            }
            FutureUtils.complete(promise, entryList);
        } else {
            FutureUtils.completeExceptionally(promise,
                    new BKTransmitException("Failed to read entries :", rc));
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        final CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new CompletableFuture<Void>();
        }
        FutureUtils.proxyTo(
            BKUtils.closeLedgers(lh),
            closeFuture
        );
        return closeFuture;
    }
}
