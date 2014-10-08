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

package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.ZeroBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a page in the LedgerCache. It holds the locations
 * (entrylogfile, offset) for entry ids.
 */
public class LedgerEntryPage {
    private final static int indexEntrySize = 8;
    private final int pageSize;
    private final int entriesPerPage;
    volatile private EntryKey entryKey = new EntryKey(-1, BookieProtocol.INVALID_ENTRY_ID);
    private final ByteBuffer page;
    volatile private boolean clean = true;
    private final AtomicInteger useCount = new AtomicInteger();
    private final AtomicInteger version = new AtomicInteger(0);
    volatile private int last = -1; // Last update position
    private final LEPStateChangeCallback callback;

    public static int getIndexEntrySize() {
        return indexEntrySize;
    }

    public LedgerEntryPage(int pageSize, int entriesPerPage) {
        this(pageSize, entriesPerPage, null);
    }

    public LedgerEntryPage(int pageSize, int entriesPerPage, LEPStateChangeCallback callback) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        page = ByteBuffer.allocateDirect(pageSize);
        this.callback = callback;
        if (null != this.callback) {
            callback.onResetInUse(this);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getLedger());
        sb.append('@');
        sb.append(getFirstEntry());
        sb.append(clean ? " clean " : " dirty ");
        sb.append(useCount.get());
        return sb.toString();
    }

    public void usePage() {
        int oldVal = useCount.getAndIncrement();
        if ((0 == oldVal) && (null != callback)) {
            callback.onSetInUse(this);
        }
    }

    public void releasePage() {
        int newUseCount = useCount.decrementAndGet();
        if (newUseCount < 0) {
            throw new IllegalStateException("Use count has gone below 0");
        }
        if ((null != callback) && (newUseCount == 0)) {
            callback.onResetInUse(this);
        }
    }

    private void checkPage() {
        if (useCount.get() <= 0) {
            throw new IllegalStateException("Page not marked in use");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LedgerEntryPage) {
            LedgerEntryPage otherLEP = (LedgerEntryPage) other;
            return otherLEP.getLedger() == getLedger() && otherLEP.getFirstEntry() == getFirstEntry();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (int)getLedger() ^ (int)(getFirstEntry());
    }

    void setClean(int versionOfCleaning) {
        this.clean = (versionOfCleaning == version.get());

        if ((null != callback) && clean) {
            callback.onSetClean(this);
        }
    }

    boolean isClean() {
        return clean;
    }

    public void setOffset(long offset, int position) {
        checkPage();
        page.putLong(position, offset);
        version.incrementAndGet();
        if (last < position/getIndexEntrySize()) {
            last = position/getIndexEntrySize();
        }
        this.clean = false;

        if (null != callback) {
            callback.onSetDirty(this);
        }
    }

    public long getOffset(int position) {
        checkPage();
        return page.getLong(position);
    }

    public void zeroPage() {
        checkPage();
        page.clear();
        ZeroBuffer.put(page);
        last = -1;
        clean = true;
    }

    public void readPage(FileInfo fi) throws IOException {
        checkPage();
        page.clear();
        while(page.remaining() != 0) {
            if (fi.read(page, getFirstEntryPosition()) <= 0) {
                throw new IOException("Short page read of ledger " + getLedger()
                                + " tried to get " + page.capacity() + " from position " + getFirstEntryPosition()
                                + " still need " + page.remaining());
            }
        }
        last = getLastEntryIndex();
        clean = true;
    }

    public ByteBuffer getPageToWrite() {
        checkPage();
        page.clear();
        return page;
    }

    long getLedger() {
        return entryKey.getLedgerId();
    }

    int getVersion() {
        return version.get();
    }

    public EntryKey getEntryKey() {
        return entryKey;
    }

    void setLedgerAndFirstEntry(long ledgerId, long firstEntry) {
        if (firstEntry % entriesPerPage != 0) {
            throw new IllegalArgumentException(firstEntry + " is not a multiple of " + entriesPerPage);
        }
        this.entryKey = new EntryKey(ledgerId, firstEntry);
    }
    long getFirstEntry() {
        return entryKey.getEntryId();
    }

    long getMaxPossibleEntry() {
        return entryKey.getEntryId() + entriesPerPage;
    }

    long getFirstEntryPosition() {
        return entryKey.getEntryId() * indexEntrySize;
    }

    public boolean inUse() {
        return useCount.get() > 0;
    }

    private int getLastEntryIndex() {
        for(int i = entriesPerPage - 1; i >= 0; i--) {
            if (getOffset(i*getIndexEntrySize()) > 0) {
                return i;
            }
        }
        return -1;
    }

    public long getLastEntry() {
        if (last >= 0) {
            return last + entryKey.getEntryId();
        } else {
            int index = getLastEntryIndex();
            return index >= 0 ? (index + entryKey.getEntryId()) : 0;
        }
    }
}
