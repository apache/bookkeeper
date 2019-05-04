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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a page in the LedgerCache. It holds the locations
 * (entrylogfile, offset) for entry ids.
 */
public class LedgerEntryPage implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerEntryPage.class);

    private static final int indexEntrySize = 8;
    private final int pageSize;
    private final int entriesPerPage;
    private volatile EntryKey entryKey = new EntryKey(-1, BookieProtocol.INVALID_ENTRY_ID);
    private final ByteBuffer page;
    private volatile boolean clean = true;
    private final AtomicInteger useCount = new AtomicInteger(0);
    private final AtomicInteger version = new AtomicInteger(0);
    private volatile int last = -1; // Last update position
    private final LEPStateChangeCallback callback;
    private boolean deleted;

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

    // Except for not allocating a new direct byte buffer; this should do everything that
    // the constructor does
    public void resetPage() {
        page.clear();
        ZeroBuffer.put(page);
        last = -1;
        entryKey = new EntryKey(-1, BookieProtocol.INVALID_ENTRY_ID);
        clean = true;
        useCount.set(0);
        deleted = false;
        if (null != this.callback) {
            callback.onResetInUse(this);
        }
    }

    public void markDeleted() {
        deleted = true;
        version.incrementAndGet();
    }

    public boolean isDeleted() {
        return deleted;
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

    public void releasePageNoCallback() {
        releasePageInternal(false);
    }

    public void releasePage() {
        releasePageInternal(true);
    }

    private void releasePageInternal(boolean shouldCallback) {
        int newUseCount = useCount.decrementAndGet();
        if (newUseCount < 0) {
            throw new IllegalStateException("Use count has gone below 0");
        }
        if (shouldCallback && (null != callback) && (newUseCount == 0)) {
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
        return (int) getLedger() ^ (int) (getFirstEntry());
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
        if (last < position / getIndexEntrySize()) {
            last = position / getIndexEntrySize();
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
        try {
            fi.read(page, getFirstEntryPosition(), true);
        } catch (ShortReadException sre) {
            throw new ShortReadException("Short page read of ledger " + getLedger()
                    + " tried to get " + page.capacity() + " from position "
                    + getFirstEntryPosition() + " still need " + page.remaining(), sre);
        } catch (IllegalArgumentException iae) {
            LOG.error("IllegalArgumentException when trying to read ledger {} from position {}",
                    getLedger(), getFirstEntryPosition(), iae);
            throw iae;
        }
        // make sure we don't include partial index entry
        if (page.remaining() != 0) {
            LOG.info("Short page read of ledger {} : tried to read {} bytes from position {}, but only {} bytes read.",
                    getLedger(), page.capacity(), getFirstEntryPosition(), page.position());
            if (page.position() % indexEntrySize != 0) {
                int partialIndexEntryStart = page.position() - page.position() % indexEntrySize;
                page.putLong(partialIndexEntryStart, 0L);
            }
        }
        last = getLastEntryIndex();
        clean = true;
    }

    public ByteBuffer getPageToWrite() {
        checkPage();
        page.clear();
        // Different callers to this method should be able to reasonably expect independent read pointers
        return page.duplicate();
    }

    long getLedger() {
        return entryKey.getLedgerId();
    }

    public int getVersion() {
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
        for (int i = entriesPerPage - 1; i >= 0; i--) {
            if (getOffset(i * getIndexEntrySize()) > 0) {
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

    /**
     * Interface for getEntries to propagate entry, pos pairs.
     */
    public interface EntryVisitor {
        boolean visit(long entry, long pos) throws Exception;
    }

    /**
     * Iterates over non-empty entry mappings.
     *
     * @param vis Consumer for entry position pairs.
     * @throws Exception
     */
    public void getEntries(EntryVisitor vis) throws Exception {
        // process a page
        for (int i = 0; i < entriesPerPage; i++) {
            long offset = getOffset(i * 8);
            if (offset != 0) {
                if (!vis.visit(getFirstEntry() + i, offset)) {
                    return;
                }
            }
        }
    }

    public OfLong getEntriesIterator() {
        return new OfLong() {
            long firstEntry = getFirstEntry();
            int curDiffEntry = 0;

            @Override
            public boolean hasNext() {
                while ((curDiffEntry < entriesPerPage) && (getOffset(curDiffEntry * 8) == 0)) {
                    curDiffEntry++;
                }
                return (curDiffEntry != entriesPerPage);
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                long nextEntry = firstEntry + curDiffEntry;
                curDiffEntry++;
                return nextEntry;
            }
        };
    }

    @Override
    public void close() throws Exception {
        releasePage();
    }
}
