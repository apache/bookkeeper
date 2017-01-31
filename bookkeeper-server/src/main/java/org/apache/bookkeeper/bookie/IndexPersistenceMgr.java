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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_NUM_EVICTED_LEDGERS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.NUM_OPEN_LEDGERS;

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

    final ConcurrentMap<Long, FileInfo> fileInfoCache = new ConcurrentHashMap<Long, FileInfo>();
    final int openFileLimit;
    final int pageSize;
    final int entriesPerPage;

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final SnapshotMap<Long, Boolean> activeLedgers;
    private LedgerDirsManager ledgerDirsManager;
    final LinkedList<Long> openLedgers = new LinkedList<Long>();

    // Stats
    private final Counter evictedLedgersCounter;

    public IndexPersistenceMgr(int pageSize,
                               int entriesPerPage,
                               ServerConfiguration conf,
                               SnapshotMap<Long, Boolean> activeLedgers,
                               LedgerDirsManager ledgerDirsManager,
                               StatsLogger statsLogger) throws IOException {
        this.openFileLimit = conf.getOpenFileLimit();
        this.activeLedgers = activeLedgers;
        this.ledgerDirsManager = ledgerDirsManager;
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        LOG.info("openFileLimit = {}", openFileLimit);
        // Retrieve all of the active ledgers.
        getActiveLedgers();
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());

        // Expose Stats
        evictedLedgersCounter = statsLogger.getCounter(LEDGER_CACHE_NUM_EVICTED_LEDGERS);
        statsLogger.registerGauge(NUM_OPEN_LEDGERS, new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return getNumOpenLedgers();
            }
        });
    }

    FileInfo getFileInfo(Long ledger, byte masterKey[]) throws IOException {
        FileInfo fi = fileInfoCache.get(ledger);
        if (null == fi) {
            boolean createdNewFile = false;
            File lf = null;
            synchronized (this) {
                // Check if the index file exists on disk.
                lf = findIndexFile(ledger);
                if (null == lf) {
                    if (null == masterKey) {
                        throw new Bookie.NoLedgerException(ledger);
                    }
                    // We don't have a ledger index file on disk, so create it.
                    lf = getNewLedgerIndexFile(ledger, null);
                    createdNewFile = true;
                }
            }
            fi = putFileInfo(ledger, masterKey, lf, createdNewFile);
        }

        assert null != fi;
        fi.use();
        return fi;
    }

    private FileInfo putFileInfo(Long ledger, byte masterKey[], File lf, boolean createdNewFile) throws IOException {
        FileInfo fi = new FileInfo(lf, masterKey);
        FileInfo oldFi = fileInfoCache.putIfAbsent(ledger, fi);
        if (null != oldFi) {
            // Some other thread won the race. We should delete our file if we created
            // a new one and the paths are different.
            if (createdNewFile && !oldFi.isSameFile(lf)) {
                fi.delete();
            }
            fi = oldFi;
        } else {
            if (createdNewFile) {
                // Else, we won and the active ledger manager should know about this.
                LOG.debug("New ledger index file created for ledgerId: {}", ledger);
                activeLedgers.put(ledger, true);
            }
            // Evict cached items from the file info cache if necessary
            evictFileInfoIfNecessary();
            synchronized (openLedgers) {
                openLedgers.offer(ledger);
            }
        }
        return fi;
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
            File[] grandParents = ledgerDirectory.listFiles();
            if (grandParents == null) {
                continue;
            }
            for (File grandParent : grandParents) {
                if (grandParent.isDirectory()) {
                    File[] parents = grandParent.listFiles();
                    if (parents == null) {
                        continue;
                    }
                    for (File parent : parents) {
                        if (parent.isDirectory()) {
                            File[] indexFiles = parent.listFiles();
                            if (indexFiles == null) {
                                continue;
                            }
                            for (File index : indexFiles) {
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
        fileInfoCache.remove(ledgerId);
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
        return activeLedgers.containsKey(ledgerId);
    }

    int getNumOpenLedgers() {
        return openLedgers.size();
    }

    // evict file info if necessary
    private void evictFileInfoIfNecessary() throws IOException {
        if (openLedgers.size() > openFileLimit) {
            Long ledgerToRemove;
            synchronized (openLedgers) {
                ledgerToRemove = openLedgers.poll();
            }
            if (null == ledgerToRemove) {
                // Should not reach here. We probably cleared this while the thread
                // was executing.
                return;
            }
            evictedLedgersCounter.inc();
            FileInfo fi = fileInfoCache.remove(ledgerToRemove);
            if (null == fi) {
                // Seems like someone else already closed the file.
                return;
            }
            fi.close(true);
         }
    }

    void close() throws IOException {
        for (Entry<Long, FileInfo> fileInfo : fileInfoCache.entrySet()) {
            FileInfo value = fileInfo.getValue();
            if (value != null) {
                value.close(true);
            }
        }
        fileInfoCache.clear();
    }

    Long getLastAddConfirmed(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.getLastAddConfirmed();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    long updateLastAddConfirmed(long ledgerId, long lac) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.setLastAddConfirmed(lac);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        FileInfo fi = fileInfoCache.get(ledgerId);
        if (fi == null) {
            File lf = findIndexFile(ledgerId);
            if (lf == null) {
                throw new Bookie.NoLedgerException(ledgerId);
            }
            fi = putFileInfo(ledgerId, null, lf, false);
        }
        return fi.getMasterKey();
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

    void setExplicitLac(long ledgerId, ByteBuffer lac) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            fi.setExplicitLac(lac);
            return;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    public ByteBuffer getExplicitLac(long ledgerId) {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.getExplicitLac();
        } catch (IOException e) {
            LOG.error("Exception during getLastAddConfirmed: {}", e);
            return null;
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
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskAlmostFull(File disk) {
                // Nothing to handle here. Will be handled in Bookie
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

            @Override
            public void diskWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskJustWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    private void relocateIndexFileAndFlushHeader(long ledger, FileInfo fi) throws IOException {
        File currentDir = getLedgerDirForLedger(fi);
        if (ledgerDirsManager.isDirFull(currentDir)) {
            moveLedgerIndexFile(ledger, fi);
        }
        fi.flushHeader();
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
            relocateIndexFileAndFlushHeader(ledger, fi);
        } catch (Bookie.NoLedgerException nle) {
            // ledger has been deleted
            LOG.info("No ledger {} found when flushing header.", ledger);
            return;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
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
            int[] versions = new int[entries.size()];
            try {
                fi = getFileInfo(l, null);
            } catch (Bookie.NoLedgerException nle) {
                // ledger has been deleted
                LOG.info("No ledger {} found when flushing entries.", l);
                return;
            }

            // flush the header if necessary
            relocateIndexFileAndFlushHeader(l, fi);
            int start = 0;
            long lastOffset = -1;
            for (int i = 0; i < entries.size(); i++) {
                versions[i] = entries.get(i).getVersion();
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
            for (int i = 0; i < entries.size(); i++) {
                LedgerEntryPage lep = entries.get(i);
                lep.setClean(versions[i]);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flushed ledger {} with {} pages.", l, entries.size());
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
            long rc = fi.write(buffs, entries.get(start + 0).getFirstEntryPosition());
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
            long pos = lep.getFirstEntryPosition();
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
            if (0 != size % LedgerEntryPage.getIndexEntrySize()) {
                LOG.warn("Index file of ledger {} is not aligned with index entry size.", ledgerId);
                size = size - size % LedgerEntryPage.getIndexEntrySize();
            }
            // we may not have the last entry in the cache
            if (size > lastEntry * LedgerEntryPage.getIndexEntrySize()) {
                ByteBuffer bb = ByteBuffer.allocate(pageSize);
                long position = size - pageSize;
                if (position < 0) {
                    position = 0;
                }
                fi.read(bb, position);
                bb.flip();
                long startingEntryId = position / LedgerEntryPage.getIndexEntrySize();
                for (int i = entriesPerPage - 1; i >= 0; i--) {
                    if (bb.getLong(i * LedgerEntryPage.getIndexEntrySize()) != 0) {
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
