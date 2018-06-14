/**
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
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * This contains LastAddConfirmed entryId and a LedgerEntry wanted to read.
 * It is used for readLastAddConfirmedAndEntry.
 */
public class LastConfirmedAndEntryImpl implements LastConfirmedAndEntry {

    private static final Recycler<LastConfirmedAndEntryImpl> RECYCLER = new Recycler<LastConfirmedAndEntryImpl>() {
        @Override
        protected LastConfirmedAndEntryImpl newObject(Handle<LastConfirmedAndEntryImpl> handle) {
            return new LastConfirmedAndEntryImpl(handle);
        }
    };

    public static LastConfirmedAndEntryImpl create(long lac, org.apache.bookkeeper.client.LedgerEntry entry) {
        LastConfirmedAndEntryImpl entryImpl = RECYCLER.get();
        entryImpl.lac = lac;
        if (null == entry) {
            entryImpl.entry = null;
        } else {
            entryImpl.entry = LedgerEntryImpl.create(
                entry.getLedgerId(),
                entry.getEntryId(),
                entry.getLength(),
                entry.getEntryBuffer());
        }
        return entryImpl;
    }

    private final Handle<LastConfirmedAndEntryImpl> recycleHandle;
    private Long lac;
    private LedgerEntry entry;

    public LastConfirmedAndEntryImpl(Handle<LastConfirmedAndEntryImpl> handle) {
        this.recycleHandle = handle;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastAddConfirmed() {
        return lac;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasEntry() {
        return entry != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LedgerEntry getEntry() {
        return entry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        this.lac = -1L;
        if (null != entry) {
            entry.close();
            entry = null;
        }
        recycleHandle.recycle(this);
    }
}
