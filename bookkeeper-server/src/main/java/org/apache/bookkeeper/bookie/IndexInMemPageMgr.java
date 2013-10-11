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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexInMemPageMgr {
    private final static Logger LOG = LoggerFactory.getLogger(IndexInMemPageMgr.class);

    final int pageSize;
    final int pageLimit;
    final int entriesPerPage;
    final HashMap<Long, HashMap<Long, LedgerEntryPage>> pages;

    // The number of pages that have actually been used
    private int pageCount = 0;

    // The persistence manager that this page manager uses to
    // flush and read pages
    private final IndexPersistenceMgr indexPersistenceManager;

    /**
     * the list of potentially dirty ledgers
     */
    LinkedList<Long> dirtyLedgers = new LinkedList<Long>();
    /**
     * the list of potentially clean ledgers
     */
    LinkedList<Long> cleanLedgers = new LinkedList<Long>();

    public IndexInMemPageMgr(int pageSize,
                             int entriesPerPage,
                             ServerConfiguration conf, 
                             IndexPersistenceMgr indexPersistenceManager) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        this.indexPersistenceManager = indexPersistenceManager;
        this.pages = new HashMap<Long, HashMap<Long, LedgerEntryPage>>();

        if (conf.getPageLimit() <= 0) {
            // allocate half of the memory to the page cache
            this.pageLimit = (int) ((Runtime.getRuntime().maxMemory() / 3) / this.pageSize);
        } else {
            this.pageLimit = conf.getPageLimit();
        }
        LOG.info("maxMemory = {}, pageSize = {}, pageLimit = {}", new Object[] { Runtime.getRuntime().maxMemory(),
                        pageSize, pageLimit });
    }

    /**
     * @return page size used in ledger cache
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return entries per page used in ledger cache
     */
    public int getEntriesPerPage() {
        return entriesPerPage;
    }

    /**
     * @return page limitation in ledger cache
     */
    public int getPageLimit() {
        return pageLimit;
    }

    /**
     * @return number of page used in ledger cache
     */
    public int getNumUsedPages() {
        return pageCount;
    }

    public int getNumCleanLedgers() {
        return cleanLedgers.size();
    }

    public int getNumDirtyLedgers() {
        return dirtyLedgers.size();
    }

    private void putIntoTable(HashMap<Long, HashMap<Long,LedgerEntryPage>> table, LedgerEntryPage lep) {
        HashMap<Long, LedgerEntryPage> map = table.get(lep.getLedger());
        if (map == null) {
            map = new HashMap<Long, LedgerEntryPage>();
            table.put(lep.getLedger(), map);
        }
        map.put(lep.getFirstEntry(), lep);
    }

    private static LedgerEntryPage getFromTable(HashMap<Long, HashMap<Long,LedgerEntryPage>> table,
                                                Long ledger, Long firstEntry) {
        HashMap<Long, LedgerEntryPage> map = table.get(ledger);
        if (map != null) {
            return map.get(firstEntry);
        }
        return null;
    }

    synchronized protected LedgerEntryPage getLedgerEntryPage(Long ledger, Long firstEntry, boolean onlyDirty) {
        LedgerEntryPage lep = getFromTable(pages, ledger, firstEntry);
        if (lep == null) {
            return null;
        }

        lep.usePage();

        if (onlyDirty && lep.isClean()) {
            return null;
        } else {
            return lep;
        }
    }

    /** 
     * Grab ledger entry page whose first entry is <code>pageEntry</code>.
     *
     * If the page doesn't existed before, we allocate a memory page.
     * Otherwise, we grab a clean page and read it from disk.
     *
     * @param ledger
     *          Ledger Id
     * @param pageEntry
     *          Start entry of this entry page.
     */
    private LedgerEntryPage grabLedgerEntryPage(long ledger, long pageEntry) throws IOException {
        LedgerEntryPage lep = grabCleanPage(ledger, pageEntry);
        try {
            // should update page before we put it into table
            // otherwise we would put an empty page in it
            indexPersistenceManager.updatePage(lep);
            synchronized (this) {
                putIntoTable(pages, lep);
            }
        } catch (IOException ie) {
            // if we grab a clean page, but failed to update the page
            // we are exhausting the count of ledger entry pages.
            // since this page will be never used, so we need to decrement
            // page count of ledger cache.
            lep.releasePage();
            synchronized (this) {
                --pageCount;
            }
            throw ie;
        }
        return lep;
    }

    void removePagesForLedger(long ledgerId) {
        // remove pages first to avoid page flushed when deleting file info
        synchronized (this) {
            Map<Long, LedgerEntryPage> lpages = pages.remove(ledgerId);
            if (null != lpages) {
                pageCount -= lpages.size();
                if (pageCount < 0) {
                    LOG.error("Page count of ledger cache has been decremented to be less than zero.");
                }
            }
        }
    }

    long getLastEntryInMem(long ledgerId) {
        long lastEntry = 0;
        // Find the last entry in the cache
        synchronized (this) {
            Map<Long, LedgerEntryPage> map = pages.get(ledgerId);
            if (map != null) {
                for (LedgerEntryPage lep : map.values()) {
                    if (lep.getFirstEntry() + entriesPerPage < lastEntry) {
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
        }
        return lastEntry;
    }

    private LedgerEntryPage grabCleanPage(long ledger, long entry) throws IOException {
        if (entry % entriesPerPage != 0) {
            throw new IllegalArgumentException(entry + " is not a multiple of " + entriesPerPage);
        }
        outerLoop: while (true) {
            synchronized (this) {
                if (pageCount < pageLimit) {
                    // let's see if we can allocate something
                    LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
                    lep.setLedger(ledger);
                    lep.setFirstEntry(entry);

                    // note, this will not block since it is a new page
                    lep.usePage();
                    pageCount++;
                    return lep;
                }
            }

            synchronized (cleanLedgers) {
                if (cleanLedgers.isEmpty()) {
                    flushOneOrMoreLedgers(false);
                    synchronized (this) {
                        for (Long l : pages.keySet()) {
                            cleanLedgers.add(l);
                        }
                    }
                }
                synchronized (this) {
                    // if ledgers deleted between checking pageCount and putting
                    // ledgers into cleanLedgers list, the cleanLedgers list would be empty.
                    // so give it a chance to go back to check pageCount again because
                    // deleteLedger would decrement pageCount to return the number of pages
                    // occupied by deleted ledgers.
                    if (cleanLedgers.isEmpty()) {
                        continue outerLoop;
                    }
                    Long cleanLedger = cleanLedgers.getFirst();
                    Map<Long, LedgerEntryPage> map = pages.get(cleanLedger);
                    while (map == null || map.isEmpty()) {
                        cleanLedgers.removeFirst();
                        if (cleanLedgers.isEmpty()) {
                            continue outerLoop;
                        }
                        cleanLedger = cleanLedgers.getFirst();
                        map = pages.get(cleanLedger);
                    }
                    Iterator<Map.Entry<Long, LedgerEntryPage>> it = map.entrySet().iterator();
                    LedgerEntryPage lep = it.next().getValue();
                    while ((lep.inUse() || !lep.isClean())) {
                        if (!it.hasNext()) {
                            // no clean page found in this ledger
                            cleanLedgers.removeFirst();
                            continue outerLoop;
                        }
                        lep = it.next().getValue();
                    }
                    it.remove();
                    if (map.isEmpty()) {
                        pages.remove(lep.getLedger());
                    }
                    lep.usePage();
                    lep.zeroPage();
                    lep.setLedger(ledger);
                    lep.setFirstEntry(entry);
                    return lep;
                }
            }
        }
    }

    void flushOneOrMoreLedgers(boolean doAll) throws IOException {
        synchronized (dirtyLedgers) {
            if (dirtyLedgers.isEmpty()) {
                synchronized (this) {
                    for (Long l : pages.keySet()) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Adding {} to dirty pages", Long.toHexString(l));
                        }
                        dirtyLedgers.add(l);
                    }
                }
            }
            if (dirtyLedgers.isEmpty()) {
                return;
            }

            indexPersistenceManager.relocateIndexFileIfDirFull(dirtyLedgers);

            while (!dirtyLedgers.isEmpty()) {
                Long l = dirtyLedgers.removeFirst();

                flushSpecificLedger(l);

                if (!doAll) {
                    break;
                }
                // Yield. if we are doing all the ledgers we don't want to block other flushes that
                // need to happen
                try {
                    dirtyLedgers.wait(1);
                } catch (InterruptedException e) {
                    // just pass it on
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Flush a specified ledger
     *
     * @param l 
     *          Ledger Id
     * @throws IOException
     */
    private void flushSpecificLedger(long l) throws IOException {
        LinkedList<Long> firstEntryList;
        synchronized(this) {
            HashMap<Long, LedgerEntryPage> pageMap = pages.get(l);
            if (pageMap == null || pageMap.isEmpty()) {
                indexPersistenceManager.flushLedgerHeader(l);
                return;
            }
            firstEntryList = new LinkedList<Long>();
            for(Map.Entry<Long, LedgerEntryPage> entry: pageMap.entrySet()) {
                LedgerEntryPage lep = entry.getValue();
                if (lep.isClean()) {
                    LOG.trace("Page is clean {}", lep);
                    continue;
                }
                firstEntryList.add(lep.getFirstEntry());
            }
        }

        if (firstEntryList.size() == 0) {
            LOG.debug("Nothing to flush for ledger {}.", l);
            // nothing to do
            return;
        }

        // Now flush all the pages of a ledger
        List<LedgerEntryPage> entries = new ArrayList<LedgerEntryPage>(firstEntryList.size());
        try {
            for(Long firstEntry: firstEntryList) {
                LedgerEntryPage lep = getLedgerEntryPage(l, firstEntry, true);
                if (lep != null) {
                    entries.add(lep);
                }
            }
            indexPersistenceManager.flushLedgerEntries(l, entries);
        } finally {
            for(LedgerEntryPage lep: entries) {
                lep.releasePage();
            }
        }
    }

    void putEntryOffset(long ledger, long entry, long offset) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry - offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        if (lep == null) {
            lep = grabLedgerEntryPage(ledger, pageEntry);
        }
        lep.setOffset(offset, offsetInPage * 8);
        lep.releasePage();
    }

    long getEntryOffset(long ledger, long entry) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry - offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        try {
            if (lep == null) {
                lep = grabLedgerEntryPage(ledger, pageEntry);
            }
            return lep.getOffset(offsetInPage * 8);
        } finally {
            if (lep != null) {
                lep.releasePage();
            }
        }
    }
}
