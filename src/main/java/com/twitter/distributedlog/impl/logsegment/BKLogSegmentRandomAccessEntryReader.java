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

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.Entry;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.logsegment.LogSegmentRandomAccessEntryReader;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;

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
    private Promise<Void> closePromise = null;

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
    public Future<List<Entry.Reader>> readEntries(long startEntryId, long endEntryId) {
        Promise<List<Entry.Reader>> promise = new Promise<List<Entry.Reader>>();
        lh.asyncReadEntries(startEntryId, endEntryId, this, promise);
        return promise;
    }

    Entry.Reader processReadEntry(LedgerEntry entry) throws IOException {
        return Entry.newBuilder()
                .setLogSegmentInfo(lssn, startSequenceId)
                .setEntryId(entry.getEntryId())
                .setEnvelopeEntry(envelopeEntries)
                .deserializeRecordSet(deserializeRecordSet)
                .setInputStream(entry.getEntryInputStream())
                .buildReader();
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
        Promise<List<Entry.Reader>> promise = (Promise<List<Entry.Reader>>) ctx;
        if (BKException.Code.OK == rc) {
            List<Entry.Reader> entryList = Lists.newArrayList();
            while (entries.hasMoreElements()) {
                try {
                    entryList.add(processReadEntry(entries.nextElement()));
                } catch (IOException ioe) {
                    FutureUtils.setException(promise, ioe);
                    return;
                }
            }
            FutureUtils.setValue(promise, entryList);
        } else {
            FutureUtils.setException(promise,
                    new BKTransmitException("Failed to read entries :", rc));
        }
    }

    @Override
    public Future<Void> asyncClose() {
        final Promise<Void> closeFuture;
        synchronized (this) {
            if (null != closePromise) {
                return closePromise;
            }
            closeFuture = closePromise = new Promise<Void>();
        }
        BKUtils.closeLedgers(lh).proxyTo(closeFuture);
        return closeFuture;
    }
}
