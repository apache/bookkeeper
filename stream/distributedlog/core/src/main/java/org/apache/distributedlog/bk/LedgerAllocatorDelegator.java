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
package org.apache.distributedlog.bk;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Transaction.OpListener;


/**
 * Delegator of the underlying allocator. If it owns the allocator, it takes
 * the responsibility of start the allocator and close the allocator.
 */
public class LedgerAllocatorDelegator implements LedgerAllocator {

    private final LedgerAllocator allocator;
    private final boolean ownAllocator;

    /**
     * Create an allocator's delegator.
     *
     * @param allocator
     *          the underlying allocator
     * @param ownAllocator
     *          whether to own the allocator
     */
    public LedgerAllocatorDelegator(LedgerAllocator allocator,
                                    boolean ownAllocator)
            throws IOException {
        this.allocator = allocator;
        this.ownAllocator = ownAllocator;
        if (this.ownAllocator) {
            this.allocator.start();
        }
    }

    @Override
    public void start() throws IOException {
        // no-op
    }

    @Override
    public CompletableFuture<Void> delete() {
        return FutureUtils.exception(new UnsupportedOperationException("Can't delete an allocator by delegator"));
    }

    @Override
    public void allocate() throws IOException {
        this.allocator.allocate();
    }

    @Override
    public CompletableFuture<LedgerHandle> tryObtain(Transaction<Object> txn,
                                          OpListener<LedgerHandle> listener) {
        return this.allocator.tryObtain(txn, listener);
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        if (ownAllocator) {
            return this.allocator.asyncClose();
        } else {
            return FutureUtils.value(null);
        }
    }
}
