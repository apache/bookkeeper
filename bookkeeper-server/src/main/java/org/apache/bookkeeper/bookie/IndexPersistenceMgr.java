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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class IndexPersistenceMgr {
    private final static Logger LOG = LoggerFactory.getLogger(IndexPersistenceMgr.class);

    private static final String IDX = ".idx";
    static final String RLOC = ".rloc";

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

    final HashMap<Long, FileInfo> fileInfoCache = new HashMap<Long, FileInfo>();
    final int openFileLimit;
    final int pageSize;
    final int entriesPerPage;

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final SnapshotMap<Long, Boolean> activeLedgers;
    private LedgerDirsManager ledgerDirsManager;
    final LinkedList<Long> openLedgers = new LinkedList<Long>();
    final private AtomicBoolean shouldRelocateIndexFile = new AtomicBoolean(false);

    public IndexPersistenceMgr(int pageSize,
                               int entriesPerPage,
                               ServerConfiguration conf,
                               SnapshotMap<Long, Boolean> activeLedgers,
                               LedgerDirsManager ledgerDirsManager) throws IOException {
        this.openFileLimit = conf.getOpenFileLimit();
        this.activeLedgers = activeLedgers;
        this.ledgerDirsManager = ledgerDirsManager;
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        LOG.info("openFileLimit = {}", openFileLimit);
        // Retrieve all of the active ledgers.
        getActiveLedgers();
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }

    FileInfo getFileInfo(Long ledger, byte masterKey[]) throws IOException {
        synchronized (fileInfoCache) {
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
    void removeLedger(long ledgerId) throws IOException {
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
        synchronized (fileInfoCache) {
            fileInfoCache.remove(ledgerId);
        }
        synchronized (openLedgers) {
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

    boolean ledgerExists(long ledgerId) throws IOException {
        synchronized (fileInfoCache) {
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

    int getNumOpenLedgers() {
        return openLedgers.size();
    }

    // evict file info if necessary
    private void evictFileInfoIfNecessary() throws IOException {
        synchronized (fileInfoCache) {
            if (openLedgers.size() > openFileLimit) {
                long ledgerToRemove = openLedgers.removeFirst();
                // TODO Add a statistic here, we don't care really which
                // ledger is evicted, but the rate at which they get evicted 
                fileInfoCache.remove(ledgerToRemove).close(true);
            }
        }
    }

    void close() throws IOException {
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

    byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        synchronized (fileInfoCache) {
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

    void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, masterKey);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean setFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.setFenced();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean isFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.isFenced();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    int getOpenFileLimit() {
        return openFileLimit;
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

    void relocateIndexFileIfDirFull(Collection<Long> dirtyLedgers) throws IOException {
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

    void flushLedgerHeader(long ledger) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledger, null);
            fi.flushHeader();
        } catch (Bookie.NoLedgerException nle) {
            // ledger has been deleted
            return;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
        return;
    }

    void flushLedgerEntries(long l, List<LedgerEntryPage> entries) throws IOException {
        FileInfo fi = null;
        try {
            Collections.sort(entries, new Comparator<LedgerEntryPage>() {
                @Override
                public int compare(LedgerEntryPage o1, LedgerEntryPage o2) {
                    return (int) (o1.getFirstEntry() - o2.getFirstEntry());
                }
            });
            ArrayList<Integer> versions = new ArrayList<Integer>(entries.size());
            try {
                fi = getFileInfo(l, null);
            } catch (Bookie.NoLedgerException nle) {
                // ledger has been deleted
                return;
            }

            // flush the header if necessary
            fi.flushHeader();
            int start = 0;
            long lastOffset = -1;
            for (int i = 0; i < entries.size(); i++) {
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
            if (entries.size() - start == 0 && entries.size() != 0) {
                LOG.warn("Nothing to write, but there were entries!");
            }
            writeBuffers(l, entries, fi, start, entries.size() - start);
            synchronized (this) {
                for (int i = 0; i < entries.size(); i++) {
                    LedgerEntryPage lep = entries.get(i);
                    lep.setClean(versions.get(i));
                }
            }
        } finally {
            if (fi != null) {
                fi.release();
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
        for (int j = 0; j < count; j++) {
            buffs[j] = entries.get(start + j).getPageToWrite();
            if (entries.get(start + j).getLedger() != ledger) {
                throw new IOException("Writing to " + ledger + " but page belongs to "
                                + entries.get(start + j).getLedger());
            }
        }
        long totalWritten = 0;
        while (buffs[buffs.length - 1].remaining() > 0) {
            long rc = fi.write(buffs, entries.get(start + 0).getFirstEntry() * 8);
            if (rc <= 0) {
                throw new IOException("Short write to ledger " + ledger + " rc = " + rc);
            }
            totalWritten += rc;
        }
        if (totalWritten != (long) count * (long) pageSize) {
            throw new IOException("Short write to ledger " + ledger + " wrote " + totalWritten
                            + " expected " + count * pageSize);
        }
    }

    void updatePage(LedgerEntryPage lep) throws IOException {
        if (!lep.isClean()) {
            throw new IOException("Trying to update a dirty page");
        }
        FileInfo fi = null;
        try {
            fi = getFileInfo(lep.getLedger(), null);
            long pos = lep.getFirstEntry() * 8;
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

    long getPersistEntryBeyondInMem(long ledgerId, long lastEntryInMem) throws IOException {
        FileInfo fi = null;
        long lastEntry = lastEntryInMem;
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
            if (size > lastEntry * 8) {
                ByteBuffer bb = ByteBuffer.allocate(pageSize);
                long position = size - pageSize;
                if (position < 0) {
                    position = 0;
                }
                fi.read(bb, position);
                bb.flip();
                long startingEntryId = position / 8;
                for (int i = entriesPerPage - 1; i >= 0; i--) {
                    if (bb.getLong(i * 8) != 0) {
                        if (lastEntry < startingEntryId + i) {
                            lastEntry = startingEntryId + i;
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

}
