/*
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

import static org.apache.bookkeeper.util.BookKeeperConstants.DEAD_ID;

import com.google.common.base.MoreObjects;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;

/**
 * Default implementation of {@link EntryKey}.
 */
class EntryKeyImpl extends AbstractReferenceCounted implements EntryKey {

    private static final Recycler<EntryKeyImpl> RECYCLER = new Recycler<EntryKeyImpl>() {
        @Override
        protected EntryKeyImpl newObject(Handle<EntryKeyImpl> handle) {
            return new EntryKeyImpl(handle);
        }
    };

    static EntryKeyImpl deadKey() {
        return of(DEAD_ID, DEAD_ID);
    }

    static EntryKeyImpl of(long ledgerId, long entryId) {
        EntryKeyImpl key = RECYCLER.get();
        key.setRefCnt(1);
        key.ledgerId = ledgerId;
        key.entryId = entryId;
        return key;
    }

    private final Handle<EntryKeyImpl> handle;
    private long ledgerId;
    private long entryId;

    private EntryKeyImpl(Handle<EntryKeyImpl> handle) {
        this.handle = handle;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EntryKey)) {
          return false;
        }
        EntryKey key = (EntryKey) other;
        return getLedgerId() == key.getLedgerId() && getEntryId() == key.getEntryId();
    }

    @Override
    public int hashCode() {
        return (int) (getLedgerId() * 13 ^ getEntryId() * 17);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(EntryKey.class)
            .add("lid", getLedgerId())
            .add("eid", getEntryId())
            .toString();
    }

    @Override
    protected void deallocate() {
        this.ledgerId = DEAD_ID;
        this.entryId = DEAD_ID;
        handle.recycle(this);
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }
}