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
package org.apache.bookkeeper.bookie;

import static java.lang.Long.max;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_HIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_MISS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_READ_PAGE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_INDEX_PAGES;

import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.stats.IndexInMemPageMgrStats;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DirectMemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexInMemPageMgr {
    private static final Logger LOG = LoggerFactory.getLogger(IndexInMemPageMgr.class);
    private static final ConcurrentHashMap<Long, LedgerEntryPage> EMPTY_PAGE_MAP =
            new ConcurrentHashMap<Long, LedgerEntryPage>();

    private static class InMemPageCollection implements LEPStateChangeCallback {

        final ConcurrentMap<Long, ConcurrentMap<Long, LedgerEntryPage>> pages;
        final Map<EntryKey, LedgerEntryPage> lruCleanPageMap;
        final ConcurrentLinkedQueue<LedgerEntryPage> listOfFreePages;

        // Stats
        private final IndexInMemPageMgrStats inMemPageMgrStats;

        public InMemPageCollection(StatsLogger statsLogger) {
            pages = new ConcurrentHashMap<>();
            lruCleanPageMap =
                    Collections.synchronizedMap(new LinkedHashMap<EntryKey, LedgerEntryPage>(16, 0.75f, true));
            listOfFreePages = new ConcurrentLinkedQueue<LedgerEntryPage>();
            inMemPageMgrStats = new IndexInMemPageMgrStats(statsLogger);
        }

        /**
         * Retrieve the LedgerEntryPage corresponding to the ledger and firstEntry.
         *
         * @param ledgerId Ledger id
         * @param firstEntry Id of the first entry in the page
         * @returns LedgerEntryPage if present
         */
        private LedgerEntryPage getPage(long ledgerId, long firstEntry) {
            ConcurrentMap<Long, LedgerEntryPage> map = pages.get(ledgerId);
            if (null != map) {
                return map.get(firstEntry);
            }
            return null;
        }

        /**
         * Add a LedgerEntryPage to the page map.
         *
         * @param lep Ledger Entry Page object
         */
        private LedgerEntryPage putPage(LedgerEntryPage lep) {
            // Do a get here to avoid too many new ConcurrentHashMaps() as putIntoTable is called frequently.
            ConcurrentMap<Long, LedgerEntryPage> map = pages.get(lep.getLedger());
            if (null == map) {
                ConcurrentMap<Long, LedgerEntryPage> mapToPut = new ConcurrentHashMap<Long, LedgerEntryPage>();
                map = pages.putIfAbsent(lep.getLedger(), mapToPut);
                if (null == map) {
                    map = mapToPut;
                }
            }
            LedgerEntryPage oldPage = map.putIfAbsent(lep.getFirstEntry(), lep);
            if (null == oldPage) {
                oldPage = lep;
                // Also include this in the clean page map if it qualifies.
                // Note: This is done for symmetry and correctness, however it should never
                // get exercised since we shouldn't attempt a put without the page being in use
                addToCleanPagesList(lep);
            }
            return oldPage;
        }

        /**
         * Traverse the pages for a given ledger in memory and find the highest entry amongst these pages.
         *
         * @param ledgerId Ledger id
         * @returns last entry in the in memory pages
         */
        private long getLastEntryInMem(long ledgerId) {
            long lastEntry = 0;
            // Find the last entry in the cache
            ConcurrentMap<Long, LedgerEntryPage> map = pages.get(ledgerId);
            if (map != null) {
                for (LedgerEntryPage lep: map.values()) {
                    if (lep.getMaxPossibleEntry() < lastEntry) {
                        continue;
                    }
                    lep.usePage();
                    long highest = lep.getLastEntry();
                    if (highest > lastEntry) {
                        lastEntry = highest;
                    }
                    lep.releasePage();
                }
            }
            return lastEntry;
        }

        /**
         * Removes ledger entry pages for a given ledger.
         *
         * @param ledgerId Ledger id
         * @returns number of pages removed
         */
        private void removeEntriesForALedger(long ledgerId) {
            // remove pages first to avoid page flushed when deleting file info
            ConcurrentMap<Long, LedgerEntryPage> lPages = pages.remove(ledgerId);
            if (null != lPages) {
                for (Map.Entry<Long, LedgerEntryPage> pageEntry: lPages.entrySet()) {
                    LedgerEntryPage lep = pageEntry.getValue();
                    lep.usePage();
                    lep.markDeleted();
                    lep.releasePage();
                }
            }
        }

        /**
         * Gets the list of pages in memory that have been changed and hence need to
         * be written as a part of the flush operation that is being issued.
         *
         * @param ledgerId Ledger id
         * @returns last entry in the in memory pages.
         */
        private LinkedList<Long> getFirstEntryListToBeFlushed(long ledgerId) {
            ConcurrentMap<Long, LedgerEntryPage> pageMap = pages.get(ledgerId);
            if (pageMap == null || pageMap.isEmpty()) {
                return null;
            }

            LinkedList<Long> firstEntryList = new LinkedList<Long>();
            for (ConcurrentMap.Entry<Long, LedgerEntryPage> entry: pageMap.entrySet()) {
                LedgerEntryPage lep = entry.getValue();
                if (lep.isClean()) {
                    if (!lep.inUse()) {
                        addToCleanPagesList(lep);
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Page is clean " + lep);
                    }
                } else {
                    firstEntryList.add(lep.getFirstEntry());
                }
            }
            return firstEntryList;
        }

        /**
         * Add the LedgerEntryPage to the clean page LRU map.
         *
         * @param lep Ledger Entry Page object
         */
        private void addToCleanPagesList(LedgerEntryPage lep) {
            synchronized (lruCleanPageMap) {
                if (lep.isClean() && !lep.inUse()) {
                    lruCleanPageMap.put(lep.getEntryKey(), lep);
                }
            }
        }

        /**
         * Remove the LedgerEntryPage from the clean page LRU map.
         *
         * @param lep Ledger Entry Page object
         */
        private void removeFromCleanPageList(LedgerEntryPage lep) {
            synchronized (lruCleanPageMap) {
                if (!lep.isClean() || lep.inUse()) {
                    lruCleanPageMap.remove(lep.getEntryKey());
                }
            }
        }

        /**
         * Get the set of active ledgers.
         *
         */
        Set<Long> getActiveLedgers() {
            return pages.keySet();
        }

        /**
         * Get a clean page and provision it for the specified ledger and firstEntry within the ledger.
         *
         * @param ledgerId Ledger id
         * @param firstEntry Id of the first entry in the page
         * @returns LedgerEntryPage if present
         */
        LedgerEntryPage grabCleanPage(long ledgerId, long firstEntry) {
            LedgerEntryPage lep = listOfFreePages.poll();
            if (null != lep) {
                lep.resetPage();
                lep.setLedgerAndFirstEntry(ledgerId, firstEntry);
                lep.usePage();
                return lep;
            }
            while (lruCleanPageMap.size() > 0) {
                lep = null;
                synchronized (lruCleanPageMap) {
                    Iterator<Map.Entry<EntryKey, LedgerEntryPage>> iterator = lruCleanPageMap.entrySet().iterator();

                    Map.Entry<EntryKey, LedgerEntryPage> entry = null;
                    while (iterator.hasNext()) {
                        entry = iterator.next();
                        iterator.remove();
                        if (entry.getValue().isClean() && !entry.getValue().inUse()) {
                            lep = entry.getValue();
                            break;
                        }
                    }

                    if (null == lep) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Did not find eligible page in the first pass");
                        }
                        return null;
                    }
                }

                // We found a candidate page, lets see if we can reclaim it before its re-used
                ConcurrentMap<Long, LedgerEntryPage> pageMap = pages.get(lep.getLedger());
                // Remove from map only if nothing has changed since we checked this lep.
                // Its possible for the ledger to have been deleted or the page to have already
                // been reclaimed. The page map is the definitive source of information, if anything
                // has changed we should leave this page along and continue iterating to find
                // another suitable page.
                if ((null != pageMap) && (pageMap.remove(lep.getFirstEntry(), lep))) {
                    if (!lep.isClean()) {
                        // Someone wrote to this page while we were reclaiming it.
                        pageMap.put(lep.getFirstEntry(), lep);
                        lep = null;
                    } else {
                        // Do some bookkeeping on the page table
                        pages.remove(lep.getLedger(), EMPTY_PAGE_MAP);
                        // We can now safely reset this lep and return it.
                        lep.usePage();
                        lep.zeroPage();
                        lep.setLedgerAndFirstEntry(ledgerId, firstEntry);
                        return lep;
                    }
                } else {
                    lep = null;
                }
            }
            return lep;
        }

        public void addToListOfFreePages(LedgerEntryPage lep) {
            if ((null == lep) || lep.inUse()) {
                inMemPageMgrStats.getIllegalStateResetCounter().inc();
            }
            if (null != lep) {
                listOfFreePages.add(lep);
            }
        }

        @Override
        public void onSetInUse(LedgerEntryPage lep) {
            removeFromCleanPageList(lep);
        }

        @Override
        public void onResetInUse(LedgerEntryPage lep) {
            if (!lep.isDeleted()) {
                addToCleanPagesList(lep);
            } else {
                addToListOfFreePages(lep);
            }
        }

        @Override
        public void onSetClean(LedgerEntryPage lep) {
            addToCleanPagesList(lep);
        }

        @Override
        public void onSetDirty(LedgerEntryPage lep) {
            removeFromCleanPageList(lep);
        }
    }

    final int pageSize;
    final int entriesPerPage;
    final int pageLimit;
    final InMemPageCollection pageMapAndList;

    // The number of pages that have actually been used
    private final AtomicInteger pageCount = new AtomicInteger(0);

    // The persistence manager that this page manager uses to
    // flush and read pages
    private final IndexPersistenceMgr indexPersistenceManager;

    // Stats
    private final Counter ledgerCacheHitCounter;
    private final Counter ledgerCacheMissCounter;
    private final OpStatsLogger ledgerCacheReadPageStats;

    public IndexInMemPageMgr(int pageSize,
                             int entriesPerPage,
                             ServerConfiguration conf,
                             IndexPersistenceMgr indexPersistenceManager,
                             StatsLogger statsLogger) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        this.indexPersistenceManager = indexPersistenceManager;
        this.pageMapAndList = new InMemPageCollection(statsLogger);

        long maxDirectMemory = DirectMemoryUtils.maxDirectMemory();

        if (conf.getPageLimit() <= 0) {
            // By default, allocate a third of the direct memory to the page cache
            this.pageLimit = (int) ((maxDirectMemory / 3) / this.pageSize);
        } else {
            this.pageLimit = conf.getPageLimit();
        }
        LOG.info("maxDirectMemory = {}, pageSize = {}, pageLimit = {}",
                maxDirectMemory, pageSize, pageLimit);
        // Expose Stats
        this.ledgerCacheHitCounter = statsLogger.getCounter(LEDGER_CACHE_HIT);
        this.ledgerCacheMissCounter = statsLogger.getCounter(LEDGER_CACHE_MISS);
        this.ledgerCacheReadPageStats = statsLogger.getOpStatsLogger(LEDGER_CACHE_READ_PAGE);
        // Export sampled stats for index pages, ledgers.
        statsLogger.registerGauge(
                NUM_INDEX_PAGES,
                new Gauge<Integer>() {
                    @Override
                    public Integer getDefaultValue() {
                        return 0;
                    }
                    @Override
                    public Integer getSample() {
                        return getNumUsedPages();
                    }
                }
        );
    }

    /**
     * @return page size used in ledger cache
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return number of page used in ledger cache
     */
    private int getNumUsedPages() {
        return pageCount.get();
    }

        /**
     * Get the ledger entry page for a given <i>pageEntry</i>.
     *
     * @param ledger
     *          ledger id
     * @param pageEntry
     *          first entry id of a given page
     * @return ledger entry page
     * @throws IOException
     */
    LedgerEntryPage getLedgerEntryPage(long ledger,
                                              long pageEntry) throws IOException {
        LedgerEntryPage lep = getLedgerEntryPageFromCache(ledger, pageEntry, false);
        if (lep == null) {
            ledgerCacheMissCounter.inc();
            lep = grabLedgerEntryPage(ledger, pageEntry);
        } else {
            ledgerCacheHitCounter.inc();
        }
        return lep;
    }

    LedgerEntryPage getLedgerEntryPageFromCache(long ledger,
                                                       long firstEntry,
                                                       boolean onlyDirty) {
        LedgerEntryPage lep = pageMapAndList.getPage(ledger, firstEntry);
        if (onlyDirty && null != lep && lep.isClean()) {
            return null;
        }
        if (null != lep) {
            lep.usePage();
        }
        return lep;
    }

    /**
     * Grab ledger entry page whose first entry is <code>pageEntry</code>.
     * If the page doesn't existed before, we allocate a memory page.
     * Otherwise, we grab a clean page and read it from disk.
     *
     * @param ledger Ledger Id
     * @param pageEntry Start entry of this entry page.
     */
    private LedgerEntryPage grabLedgerEntryPage(long ledger, long pageEntry) throws IOException {
        LedgerEntryPage lep = grabCleanPage(ledger, pageEntry);
        try {
            // should get the up to date page from the persistence manager
            // before we put it into table otherwise we would put
            // an empty page in it
            Stopwatch readPageStopwatch = Stopwatch.createStarted();
            boolean isNewPage = indexPersistenceManager.updatePage(lep);
            if (!isNewPage) {
                ledgerCacheReadPageStats.registerSuccessfulEvent(
                        readPageStopwatch.elapsed(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
            }
        } catch (IOException ie) {
            // if we grab a clean page, but failed to update the page
            // we should put this page in the free page list so that it
            // can be reassigned to the next grabPage request
            lep.releasePageNoCallback();
            pageMapAndList.addToListOfFreePages(lep);
            throw ie;
        }
        LedgerEntryPage oldLep;
        if (lep != (oldLep = pageMapAndList.putPage(lep))) {
            // if we grab a clean page, but failed to put it in the cache
            // we should put this page in the free page list so that it
            // can be reassigned to the next grabPage request
            lep.releasePageNoCallback();
            pageMapAndList.addToListOfFreePages(lep);
            // Increment the use count of the old lep because this is unexpected
            oldLep.usePage();
            lep = oldLep;
        }
        return lep;
    }

    void removePagesForLedger(long ledgerId) {
        pageMapAndList.removeEntriesForALedger(ledgerId);
    }

    long getLastEntryInMem(long ledgerId) {
        return pageMapAndList.getLastEntryInMem(ledgerId);
    }

    private LedgerEntryPage grabCleanPage(long ledger, long entry) throws IOException {
        if (entry % entriesPerPage != 0) {
            throw new IllegalArgumentException(entry + " is not a multiple of " + entriesPerPage);
        }

        while (true) {
            boolean canAllocate = false;
            if (pageCount.incrementAndGet() <= pageLimit) {
                canAllocate = true;
            } else {
                pageCount.decrementAndGet();
            }

            if (canAllocate) {
                LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage, pageMapAndList);
                lep.setLedgerAndFirstEntry(ledger, entry);
                lep.usePage();
                return lep;
            }

            LedgerEntryPage lep = pageMapAndList.grabCleanPage(ledger, entry);
            if (null != lep) {
                return lep;
            }
            LOG.info("Could not grab a clean page for ledger {}, entry {}, force flushing dirty ledgers.",
                    ledger, entry);
            flushOneOrMoreLedgers(false);
        }
    }

    void flushOneOrMoreLedgers(boolean doAll) throws IOException {
        List<Long> ledgersToFlush = new ArrayList<>(pageMapAndList.getActiveLedgers());
        for (Long potentiallyDirtyLedger : ledgersToFlush) {
            try {
                flushSpecificLedger(potentiallyDirtyLedger);
            } catch (Bookie.NoLedgerException e) {
                continue;
            }
            if (!doAll) {
                break;
            }
        }
    }

    /**
     * Flush a specified ledger.
     *
     * @param ledger Ledger Id
     * @throws IOException
     */
    private void flushSpecificLedger(long ledger) throws IOException {
        LinkedList<Long> firstEntryList = pageMapAndList.getFirstEntryListToBeFlushed(ledger);

        // flush ledger index file header if necessary
        indexPersistenceManager.flushLedgerHeader(ledger);

        if (null == firstEntryList || firstEntryList.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Nothing to flush for ledger {}.", ledger);
            }
            // nothing to do
            return;
        }

        // Now flush all the pages of a ledger
        List<LedgerEntryPage> entries = new ArrayList<LedgerEntryPage>(firstEntryList.size());
        try {
            for (Long firstEntry: firstEntryList) {
                LedgerEntryPage lep = getLedgerEntryPageFromCache(ledger, firstEntry, true);
                if (lep != null) {
                    entries.add(lep);
                }
            }
            indexPersistenceManager.flushLedgerEntries(ledger, entries);
        } finally {
            for (LedgerEntryPage lep: entries) {
                lep.releasePage();
            }
        }
    }

    void putEntryOffset(long ledger, long entry, long offset) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry - offsetInPage;
        LedgerEntryPage lep = null;
        try {
            lep = getLedgerEntryPage(ledger, pageEntry);
            assert lep != null;
            lep.setOffset(offset, offsetInPage * LedgerEntryPage.getIndexEntrySize());
        } catch (FileInfo.FileInfoDeletedException e) {
            throw new Bookie.NoLedgerException(ledger);
        } finally {
            if (null != lep) {
                lep.releasePage();
            }
        }
    }

    long getEntryOffset(long ledger, long entry) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry - offsetInPage;
        LedgerEntryPage lep = null;
        try {
            lep = getLedgerEntryPage(ledger, pageEntry);
            return lep.getOffset(offsetInPage  * LedgerEntryPage.getIndexEntrySize());
        } finally {
            if (lep != null) {
                lep.releasePage();
            }
        }
    }

    /**
     * Represents a page of the index.
     */
    private class PageEntriesImpl implements LedgerCache.PageEntries {
        final long ledgerId;
        final long initEntry;

        PageEntriesImpl(long ledgerId, long initEntry) {
            this.ledgerId = ledgerId;
            this.initEntry = initEntry;
        }

        @Override
        public LedgerEntryPage getLEP() throws IOException {
            return getLedgerEntryPage(ledgerId, initEntry);
        }

        @Override
        public long getFirstEntry() {
            return initEntry;
        }

        @Override
        public long getLastEntry() {
            return initEntry + entriesPerPage;
        }
    }

    /**
     * Iterable over index pages -- returns PageEntries rather than individual
     * entries because getEntries() above needs to be able to throw an IOException.
     */
    private class PageEntriesIterableImpl implements LedgerCache.PageEntriesIterable {
        final long ledgerId;
        final FileInfoBackingCache.CachedFileInfo fi;
        final long totalEntries;

        long curEntry = 0;

        PageEntriesIterableImpl(long ledgerId) throws IOException {
            this.ledgerId = ledgerId;
            this.fi = indexPersistenceManager.getFileInfo(ledgerId, null);
            this.totalEntries = max(entriesPerPage * (fi.size() / pageSize), getLastEntryInMem(ledgerId));
        }

        @Override
        public Iterator<LedgerCache.PageEntries> iterator() {
            return new Iterator<LedgerCache.PageEntries>() {
                @Override
                public boolean hasNext() {
                    return curEntry < totalEntries;
                }

                @Override
                public LedgerCache.PageEntries next() {
                    LedgerCache.PageEntries next = new PageEntriesImpl(ledgerId, curEntry);
                    curEntry += entriesPerPage;
                    return next;
                }
            };
        }

        @Override
        public void close() {
            fi.release();
        }
    }

    /**
     * Return iterator over pages for mapping entries to entry loggers.
     * @param ledgerId
     * @return Iterator over pages
     * @throws IOException
     */
    public LedgerCache.PageEntriesIterable listEntries(long ledgerId) throws IOException {
        return new PageEntriesIterableImpl(ledgerId);
    }
}
