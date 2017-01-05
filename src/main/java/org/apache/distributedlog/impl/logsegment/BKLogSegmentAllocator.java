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

import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.util.Allocator;
import org.apache.distributedlog.util.Transaction;
import com.twitter.util.Future;
import org.apache.bookkeeper.client.LedgerHandle;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.io.IOException;

/**
 * Allocate log segments
 */
class BKLogSegmentAllocator implements Allocator<LogSegmentEntryWriter, Object> {

    private static class NewLogSegmentEntryWriterFn extends AbstractFunction1<LedgerHandle, LogSegmentEntryWriter> {

        static final Function1<LedgerHandle, LogSegmentEntryWriter> INSTANCE =
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
    public Future<LogSegmentEntryWriter> tryObtain(Transaction<Object> txn,
                                                   final Transaction.OpListener<LogSegmentEntryWriter> listener) {
        return allocator.tryObtain(txn, new Transaction.OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle lh) {
                listener.onCommit(new BKLogSegmentEntryWriter(lh));
            }

            @Override
            public void onAbort(Throwable t) {
                listener.onAbort(t);
            }
        }).map(NewLogSegmentEntryWriterFn.INSTANCE);
    }

    @Override
    public Future<Void> asyncClose() {
        return allocator.asyncClose();
    }

    @Override
    public Future<Void> delete() {
        return allocator.delete();
    }
}
