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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of LedgerCache interface.
 * This class serves two purposes.
 */
public class LedgerCacheImpl implements LedgerCache {
    private final static Logger LOG = LoggerFactory.getLogger(LedgerCacheImpl.class);
    private static final String IDX = ".idx";
    static final String RLOC = ".rloc";

    private LedgerDirsManager ledgerDirsManager;
    final private AtomicBoolean shouldRelocateIndexFile = new AtomicBoolean(false);

    public LedgerCacheImpl(ServerConfiguration conf, SnapshotMap<Long, Boolean> activeLedgers,
            LedgerDirsManager ledgerDirsManager)
            throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        this.openFileLimit = conf.getOpenFileLimit();
        this.pageSize = conf.getPageSize();
        this.entriesPerPage = pageSize / 8;

        if (conf.getPageLimit() <= 0) {
            // allocate half of the memory to the page cache
            this.pageLimit = (int)((Runtime.getRuntime().maxMemory() / 3) / this.pageSize);
        } else {
            this.pageLimit = conf.getPageLimit();
        }
        LOG.info("maxMemory = " + Runtime.getRuntime().maxMemory());
        LOG.info("openFileLimit is " + openFileLimit + ", pageSize is " + pageSize + ", pageLimit is " + pageLimit);
        this.activeLedgers = activeLedgers;
        // Retrieve all of the active ledgers.
        getActiveLedgers();
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }
    /**
     * the list of potentially clean ledgers
     */
    LinkedList<Long> cleanLedgers = new LinkedList<Long>();

    /**
     * the list of potentially dirty ledgers
     */
    LinkedList<Long> dirtyLedgers = new LinkedList<Long>();

    HashMap<Long, FileInfo> fileInfoCache = new HashMap<Long, FileInfo>();

    LinkedList<Long> openLedgers = new LinkedList<Long>();

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final SnapshotMap<Long, Boolean> activeLedgers;

    final int openFileLimit;
    final int pageSize;
    final int pageLimit;
    final int entriesPerPage;

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

    // The number of pages that have actually been used
    private int pageCount = 0;
    HashMap<Long, HashMap<Long,LedgerEntryPage>> pages = new HashMap<Long, HashMap<Long,LedgerEntryPage>>();

    /**
     * @return number of page used in ledger cache
     */
    public int getNumUsedPages() {
        return pageCount;
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
            updatePage(lep);
            synchronized(this) {
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

    @Override
    public void putEntryOffset(long ledger, long entry, long offset) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry-offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        if (lep == null) {
            lep = grabLedgerEntryPage(ledger, pageEntry); 
        }
        if (lep != null) {
            lep.setOffset(offset, offsetInPage*8);
            lep.releasePage();
            return;
        }
    }

    @Override
    public long getEntryOffset(long ledger, long entry) throws IOException {
        int offsetInPage = (int) (entry%entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry-offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        try {
            if (lep == null) {
                lep = grabLedgerEntryPage(ledger, pageEntry);
            }
            return lep.getOffset(offsetInPage*8);
        } finally {
            if (lep != null) {
                lep.releasePage();
            }
        }
    }

    @VisibleForTesting
    public static final String getLedgerName(long ledgerId) {
        int parent = (int) (ledgerId & 0xff);
        int grandParent = (int) ((ledgerId & 0xff00) >> 8);
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(grandParent));
        sb.append('/');
        sb.append(Integer.toHexString(parent));
        sb.append('/');
        sb.append(Long.toHexString(ledgerId));
        sb.append(IDX);
        return sb.toString();
    }

    FileInfo getFileInfo(Long ledger, byte masterKey[]) throws IOException {
        synchronized(fileInfoCache) {
            FileInfo fi = fileInfoCache.get(ledger);
            if (fi == null) {
                File lf = findIndexFile(ledger);
                if (lf == null) {
                    if (masterKey == null) {
                        throw new Bookie.NoLedgerException(ledger);
                    }
                    lf = getNewLedgerIndexFile(ledger, null);
                    // A new ledger index file has been created for this Bookie.
                    // Add this new ledger to the set of active ledgers.
                    LOG.debug("New ledger index file created for ledgerId: {}", ledger);
                    activeLedgers.put(ledger, true);
                }
                evictFileInfoIfNecessary();
                fi = new FileInfo(lf, masterKey);
                fileInfoCache.put(ledger, fi);
                openLedgers.add(ledger);
            }
            if (fi != null) {
                fi.use();
            }
            return fi;
        }
    }

    /**
     * Get a new index file for ledger excluding directory <code>excludedDir</code>.
     *
     * @param ledger
     *          Ledger id.
     * @param excludedDir
     *          The ledger directory to exclude.
     * @return new index file object.
     * @throws NoWritableLedgerDirException if there is no writable dir available.
     */
    private File getNewLedgerIndexFile(Long ledger, File excludedDir)
    throws NoWritableLedgerDirException {
        File dir = ledgerDirsManager.pickRandomWritableDir(excludedDir);
        String ledgerName = getLedgerName(ledger);
        return new File(dir, ledgerName);
    }

    private void updatePage(LedgerEntryPage lep) throws IOException {
        if (!lep.isClean()) {
            throw new IOException("Trying to update a dirty page");
        }
        FileInfo fi = null;
        try {
            fi = getFileInfo(lep.getLedger(), null);
            long pos = lep.getFirstEntry()*8;
            if (pos >= fi.size()) {
                lep.zeroPage();
            } else {
                lep.readPage(fi);
            }
        } finally {
            if (fi != null) {
                fi.release();
            }
        }
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new entry
                // log.
                shouldRelocateIndexFile.set(true);
            }

            @Override
            public void diskFailed(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void allDisksFull() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void fatalError() {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    @Override
    public void flushLedger(boolean doAll) throws IOException {
        synchronized(dirtyLedgers) {
            if (dirtyLedgers.isEmpty()) {
                synchronized(this) {
                    for(Long l: pages.keySet()) {
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

            if (shouldRelocateIndexFile.get()) {
                // if some new dir detected as full, then move all corresponding
                // open index files to new location
                for (Long l : dirtyLedgers) {
                    FileInfo fi = null;
                    try {
                        fi = getFileInfo(l, null);
                        File currentDir = getLedgerDirForLedger(fi);
                        if (ledgerDirsManager.isDirFull(currentDir)) {
                            moveLedgerIndexFile(l, fi);
                        }
                    } finally {
                        if (null != fi) {
                            fi.release();
                        }
                    }
                }
                shouldRelocateIndexFile.set(false);
            }

            while(!dirtyLedgers.isEmpty()) {
                Long l = dirtyLedgers.removeFirst();

                flushLedger(l);

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
     * Get the ledger directory that the ledger index belongs to.
     *
     * @param fi File info of a ledger
     * @return ledger directory that the ledger belongs to.
     */
    private File getLedgerDirForLedger(FileInfo fi) {
        return fi.getLf().getParentFile().getParentFile().getParentFile();
    }

    private void moveLedgerIndexFile(Long l, FileInfo fi) throws NoWritableLedgerDirException, IOException {
        File newLedgerIndexFile = getNewLedgerIndexFile(l, getLedgerDirForLedger(fi));
        fi.moveToNewLocation(newLedgerIndexFile, fi.getSizeSinceLastwrite());
    }

    /**
     * Flush a specified ledger
     *
     * @param l
     *          Ledger Id
     * @throws IOException
     */
    private void flushLedger(long l) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(l, null);
            flushLedger(l, fi);
        } catch (Bookie.NoLedgerException nle) {
            // ledger has been deleted
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    private void flushLedger(long l, FileInfo fi) throws IOException {
        LinkedList<Long> firstEntryList;
        synchronized(this) {
            HashMap<Long, LedgerEntryPage> pageMap = pages.get(l);
            if (pageMap == null || pageMap.isEmpty()) {
                fi.flushHeader();
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
            Collections.sort(entries, new Comparator<LedgerEntryPage>() {
                    @Override
                    public int compare(LedgerEntryPage o1, LedgerEntryPage o2) {
                    return (int)(o1.getFirstEntry()-o2.getFirstEntry());
                    }
                    });
            ArrayList<Integer> versions = new ArrayList<Integer>(entries.size());
            // flush the header if necessary
            fi.flushHeader();
            int start = 0;
            long lastOffset = -1;
            for(int i = 0; i < entries.size(); i++) {
                versions.add(i, entries.get(i).getVersion());
                if (lastOffset != -1 && (entries.get(i).getFirstEntry() - lastOffset) != entriesPerPage) {
                    // send up a sequential list
                    int count = i - start;
                    if (count == 0) {
                        LOG.warn("Count cannot possibly be zero!");
                    }
                    writeBuffers(l, entries, fi, start, count);
                    start = i;
                }
                lastOffset = entries.get(i).getFirstEntry();
            }
            if (entries.size()-start == 0 && entries.size() != 0) {
                LOG.warn("Nothing to write, but there were entries!");
            }
            writeBuffers(l, entries, fi, start, entries.size()-start);
            synchronized(this) {
                for(int i = 0; i < entries.size(); i++) {
                    LedgerEntryPage lep = entries.get(i);
                    lep.setClean(versions.get(i));
                }
            }
        } finally {
            for(LedgerEntryPage lep: entries) {
                lep.releasePage();
            }
        }
    }

    private void writeBuffers(Long ledger,
                              List<LedgerEntryPage> entries, FileInfo fi,
                              int start, int count) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Writing {} buffers of {}", count, Long.toHexString(ledger));
        }
        if (count == 0) {
            return;
        }
        ByteBuffer buffs[] = new ByteBuffer[count];
        for(int j = 0; j < count; j++) {
            buffs[j] = entries.get(start+j).getPageToWrite();
            if (entries.get(start+j).getLedger() != ledger) {
                throw new IOException("Writing to " + ledger + " but page belongs to "
                                      + entries.get(start+j).getLedger());
            }
        }
        long totalWritten = 0;
        while(buffs[buffs.length-1].remaining() > 0) {
            long rc = fi.write(buffs, entries.get(start+0).getFirstEntry()*8);
            if (rc <= 0) {
                throw new IOException("Short write to ledger " + ledger + " rc = " + rc);
            }
            totalWritten += rc;
        }
        if (totalWritten != (long)count * (long)pageSize) {
            throw new IOException("Short write to ledger " + ledger + " wrote " + totalWritten
                                  + " expected " + count * pageSize);
        }
    }
    private LedgerEntryPage grabCleanPage(long ledger, long entry) throws IOException {
        if (entry % entriesPerPage != 0) {
            throw new IllegalArgumentException(entry + " is not a multiple of " + entriesPerPage);
        }
        outerLoop:
        while(true) {
            synchronized(this) {
                if (pageCount  < pageLimit) {
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

            synchronized(cleanLedgers) {
                if (cleanLedgers.isEmpty()) {
                    flushLedger(false);
                    synchronized(this) {
                        for(Long l: pages.keySet()) {
                            cleanLedgers.add(l);
                        }
                    }
                }
                synchronized(this) {
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
                    while((lep.inUse() || !lep.isClean())) {
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

    @Override
    public long getLastEntry(long ledgerId) throws IOException {
        long lastEntry = 0;
        // Find the last entry in the cache
        synchronized(this) {
            Map<Long, LedgerEntryPage> map = pages.get(ledgerId);
            if (map != null) {
                for(LedgerEntryPage lep: map.values()) {
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

        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            long size = fi.size();
            // make sure the file size is aligned with index entry size
            // otherwise we may read incorret data
            if (0 != size % 8) {
                LOG.warn("Index file of ledger {} is not aligned with index entry size.", ledgerId);
                size = size - size % 8;
            }
            // we may not have the last entry in the cache
            if (size > lastEntry*8) {
                ByteBuffer bb = ByteBuffer.allocate(getPageSize());
                long position = size - getPageSize();
                if (position < 0) {
                    position = 0;
                }
                fi.read(bb, position);
                bb.flip();
                long startingEntryId = position/8;
                for(int i = getEntriesPerPage()-1; i >= 0; i--) {
                    if (bb.getLong(i*8) != 0) {
                        if (lastEntry < startingEntryId+i) {
                            lastEntry = startingEntryId+i;
                        }
                        break;
                    }
                }
            }
        } finally {
            if (fi != null) {
                fi.release();
            }
        }

        return lastEntry;
    }

    /**
     * This method will look within the ledger directories for the ledger index
     * files. That will comprise the set of active ledgers this particular
     * BookieServer knows about that have not yet been deleted by the BookKeeper
     * Client. This is called only once during initialization.
     */
    private void getActiveLedgers() throws IOException {
        // Ledger index files are stored in a file hierarchy with a parent and
        // grandParent directory. We'll have to go two levels deep into these
        // directories to find the index files.
        for (File ledgerDirectory : ledgerDirsManager.getAllLedgerDirs()) {
            for (File grandParent : ledgerDirectory.listFiles()) {
                if (grandParent.isDirectory()) {
                    for (File parent : grandParent.listFiles()) {
                        if (parent.isDirectory()) {
                            for (File index : parent.listFiles()) {
                                if (!index.isFile()
                                        || (!index.getName().endsWith(IDX) && !index.getName().endsWith(RLOC))) {
                                    continue;
                                }

                                // We've found a ledger index file. The file
                                // name is the HexString representation of the
                                // ledgerId.
                                String ledgerIdInHex = index.getName().replace(RLOC, "").replace(IDX, "");
                                if (index.getName().endsWith(RLOC)) {
                                    if (findIndexFile(Long.parseLong(ledgerIdInHex)) != null) {
                                        if (!index.delete()) {
                                            LOG.warn("Deleting the rloc file " + index + " failed");
                                        }
                                        continue;
                                    } else {
                                        File dest = new File(index.getParentFile(), ledgerIdInHex + IDX);
                                        if (!index.renameTo(dest)) {
                                            throw new IOException("Renaming rloc file " + index
                                                    + " to index file has failed");
                                        }
                                    }
                                }
                                activeLedgers.put(Long.parseLong(ledgerIdInHex, 16), true);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * This method is called whenever a ledger is deleted by the BookKeeper Client
     * and we want to remove all relevant data for it stored in the LedgerCache.
     */
    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        LOG.debug("Deleting ledgerId: {}", ledgerId);

        // remove pages first to avoid page flushed when deleting file info
        synchronized(this) {
            Map<Long, LedgerEntryPage> lpages = pages.remove(ledgerId);
            if (null != lpages) {
                pageCount -= lpages.size();
                if (pageCount < 0) {
                    LOG.error("Page count of ledger cache has been decremented to be less than zero.");
                }
            }
        }
        // Delete the ledger's index file and close the FileInfo
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            fi.close(false);
            fi.delete();
        } finally {
            // should release use count
            // otherwise the file channel would not be closed.
            if (null != fi) {
                fi.release();
            }
        }

        // Remove it from the active ledger manager
        activeLedgers.remove(ledgerId);

        // Now remove it from all the other lists and maps.
        // These data structures need to be synchronized first before removing entries.
        synchronized(fileInfoCache) {
            fileInfoCache.remove(ledgerId);
        }
        synchronized(cleanLedgers) {
            cleanLedgers.remove(ledgerId);
        }
        synchronized(dirtyLedgers) {
            dirtyLedgers.remove(ledgerId);
        }
        synchronized(openLedgers) {
            openLedgers.remove(ledgerId);
        }
    }

    private File findIndexFile(long ledgerId) throws IOException {
        String ledgerName = getLedgerName(ledgerId);
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File lf = new File(d, ledgerName);
            if (lf.exists()) {
                return lf;
            }
        }
        return null;
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        synchronized(fileInfoCache) {
            FileInfo fi = fileInfoCache.get(ledgerId);
            if (fi == null) {
                File lf = findIndexFile(ledgerId);
                if (lf == null) {
                    throw new Bookie.NoLedgerException(ledgerId);
                }
                evictFileInfoIfNecessary();        
                fi = new FileInfo(lf, null);
                byte[] key = fi.getMasterKey();
                fileInfoCache.put(ledgerId, fi);
                openLedgers.add(ledgerId);
                return key;
            }
            return fi.getMasterKey();
        }
    }

    // evict file info if necessary
    private void evictFileInfoIfNecessary() throws IOException {
        synchronized (fileInfoCache) {
            if (openLedgers.size() > openFileLimit) {
                long ledgerToRemove = openLedgers.removeFirst();
                // TODO Add a statistic here, we don't care really which
                // ledger is evicted, but the rate at which they get evicted
                LOG.debug("Ledger {} is evicted from file info cache.",
                          ledgerToRemove);
                FileInfo fi = fileInfoCache.remove(ledgerToRemove);
                if (fi != null) {
                    fi.close(true);
                }
            }
        }
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            if (null != fi) {
                return fi.setFenced();
            }
            return false;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            if (null != fi) {
                return fi.isFenced();
            }
            return false;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, masterKey);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        synchronized(fileInfoCache) {
            FileInfo fi = fileInfoCache.get(ledgerId);
            if (fi == null) {
                File lf = findIndexFile(ledgerId);
                if (lf == null) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public LedgerCacheBean getJMXBean() {
        return new LedgerCacheBean() {
            @Override
            public String getName() {
                return "LedgerCache";
            }

            @Override
            public boolean isHidden() {
                return false;
            }

            @Override
            public int getPageCount() {
                return LedgerCacheImpl.this.getNumUsedPages();
            }

            @Override
            public int getPageSize() {
                return LedgerCacheImpl.this.getPageSize();
            }

            @Override
            public int getOpenFileLimit() {
                return openFileLimit;
            }

            @Override
            public int getPageLimit() {
                return LedgerCacheImpl.this.getPageLimit();
            }

            @Override
            public int getNumCleanLedgers() {
                return cleanLedgers.size();
            }

            @Override
            public int getNumDirtyLedgers() {
                return dirtyLedgers.size();
            }

            @Override
            public int getNumOpenLedgers() {
                return openLedgers.size();
            }
        };
    }

    @Override
    public void close() throws IOException {
        synchronized (fileInfoCache) {
            for (Entry<Long, FileInfo> fileInfo : fileInfoCache.entrySet()) {
                FileInfo value = fileInfo.getValue();
                if (value != null) {
                    value.close(true);
                }
            }
            fileInfoCache.clear();
        }
    }
}
