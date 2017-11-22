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

import io.netty.util.Recycler;
import java.util.Iterator;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerEntry;

public class LedgerEntriesImpl implements org.apache.bookkeeper.client.api.LedgerEntries {
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
            for (LedgerEntry entry : entries) {
                entry.getEntryBuffer().release(1);
            }
        }
    }

    /**
     * Create ledger entries.
     *
     * @param entries the entries with ordering
     * @return the LedgerEntriesImpl
     */
    public static LedgerEntriesImpl create(List<LedgerEntry> entries) {
        LedgerEntriesImpl ledgerEntries = RECYCLER.get();
        ledgerEntries.entries = entries;
        return ledgerEntries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LedgerEntry getEntry(long entryId) {
        long firstId = entries.get(0).getEntryId();
        long lastId = entries.get(entries.size() - 1).getEntryId();
        if (entryId < firstId || entryId > lastId) {
            return null;
        }
        return entries.get((int)(entryId - firstId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<LedgerEntry> iterator() {
        return entries.iterator();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<LedgerEntry> retainIterator() {
        // Retains the references for all the entries, caller is responsible for releasing.
        entries.forEach(ledgerEntry -> ledgerEntry.getEntryBuffer().retain());

        return iterator();
    }

    @Override
    public void close(){
        recycle();
    }
}
