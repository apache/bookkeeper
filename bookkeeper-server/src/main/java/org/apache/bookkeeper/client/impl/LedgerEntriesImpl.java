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
package org.apache.bookkeeper.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.util.Recycler;

import java.util.Iterator;
import java.util.List;

import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * Ledger entries implementation. It is a simple wrap of a list of ledger entries.
 */
public class LedgerEntriesImpl implements LedgerEntries {
    private List<LedgerEntry> entries;
    private final Recycler.Handle<LedgerEntriesImpl> recyclerHandle;

    private LedgerEntriesImpl(Recycler.Handle<LedgerEntriesImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<LedgerEntriesImpl> RECYCLER = new Recycler<LedgerEntriesImpl>() {
        @Override
        protected LedgerEntriesImpl newObject(Recycler.Handle<LedgerEntriesImpl> handle) {
            return new LedgerEntriesImpl(handle);
        }
    };

    private void recycle() {
        releaseByteBuf();
        recyclerHandle.recycle(this);
    }

    private void releaseByteBuf() {
        if (entries != null) {
            entries.forEach(LedgerEntry::close);
            entries.clear();
            entries = null;
        }
    }

    /**
     * Create ledger entries.
     *
     * @param entries the entries with ordering
     * @return the LedgerEntriesImpl
     */
    public static LedgerEntriesImpl create(List<LedgerEntry> entries) {
        checkArgument(!entries.isEmpty(), "entries for create should not be empty.");
        LedgerEntriesImpl ledgerEntries = RECYCLER.get();
        ledgerEntries.entries = entries;
        return ledgerEntries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LedgerEntry getEntry(long entryId) {
        checkNotNull(entries, "entries has been recycled");
        long firstId = entries.get(0).getEntryId();
        long lastId = entries.get(entries.size() - 1).getEntryId();
        if (entryId < firstId || entryId > lastId) {
            throw new IndexOutOfBoundsException("required index: " + entryId
                + " is out of bounds: [ " + firstId + ", " + lastId + " ].");
        }
        return entries.get((int) (entryId - firstId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<LedgerEntry> iterator() {
        checkNotNull(entries, "entries has been recycled");
        return entries.iterator();
    }

    @Override
    public void close(){
        recycle();
    }
}
