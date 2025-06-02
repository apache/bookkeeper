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

package org.apache.bookkeeper.proto;

import io.netty.util.Recycler;

class EntryCompletionKey extends CompletionKey {
    private final Recycler.Handle<EntryCompletionKey> recyclerHandle;
    long ledgerId;
    long entryId;

    static EntryCompletionKey acquireV2Key(long ledgerId, long entryId,
                                           BookkeeperProtocol.OperationType operationType) {
        EntryCompletionKey key = V2_KEY_RECYCLER.get();
        key.reset(ledgerId, entryId, operationType);
        return key;
    }

    private EntryCompletionKey(Recycler.Handle<EntryCompletionKey> handle) {
        super(null);
        this.recyclerHandle = handle;
    }

    void reset(long ledgerId, long entryId, BookkeeperProtocol.OperationType operationType) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.operationType = operationType;
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof EntryCompletionKey)) {
            return  false;
        }
        EntryCompletionKey that = (EntryCompletionKey) object;
        return this.entryId == that.entryId
                && this.ledgerId == that.ledgerId
                && this.operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(ledgerId) * 31 + Long.hashCode(entryId);
    }

    @Override
    public String toString() {
        return String.format("%d:%d %s", ledgerId, entryId, operationType);
    }

    public void release() {
        recyclerHandle.recycle(this);
    }

    private static final Recycler<EntryCompletionKey> V2_KEY_RECYCLER =
            new Recycler<EntryCompletionKey>() {
                @Override
                protected EntryCompletionKey newObject(
                        Recycler.Handle<EntryCompletionKey> handle) {
                    return new EntryCompletionKey(handle);
                }
            };
}
