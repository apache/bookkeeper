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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.util.Allocator;
import org.apache.distributedlog.util.Transaction;

/**
 * Allocate log segments.
 */
class BKLogSegmentAllocator implements Allocator<LogSegmentEntryWriter, Object> {

    private static class NewLogSegmentEntryWriterFn implements Function<LedgerHandle, LogSegmentEntryWriter> {

        static final Function<LedgerHandle, LogSegmentEntryWriter> INSTANCE =
                new NewLogSegmentEntryWriterFn();

        private NewLogSegmentEntryWriterFn() {}

        @Override
        public LogSegmentEntryWriter apply(LedgerHandle lh) {
            return new BKLogSegmentEntryWriter(lh);
        }
    }

    LedgerAllocator allocator;

    BKLogSegmentAllocator(LedgerAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void allocate() throws IOException {
        allocator.allocate();
    }

    @Override
    public CompletableFuture<LogSegmentEntryWriter> tryObtain(Transaction<Object> txn, final
    Transaction.OpListener<LogSegmentEntryWriter> listener) {
        return allocator.tryObtain(txn, new Transaction.OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle lh) {
                listener.onCommit(new BKLogSegmentEntryWriter(lh));
            }

            @Override
            public void onAbort(Throwable t) {
                listener.onAbort(t);
            }
        }).thenApply(NewLogSegmentEntryWriterFn.INSTANCE);
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return allocator.asyncClose();
    }

    @Override
    public CompletableFuture<Void> delete() {
        return allocator.delete();
    }
}
