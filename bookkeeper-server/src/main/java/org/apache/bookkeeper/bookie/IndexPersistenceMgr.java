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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.FileInfoBackingCache.CachedFileInfo;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.IndexPersistenceMgrStats;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code IndexPersistenceMgr} is responsible for managing the persistence state for the index in a bookie.
 */
public class IndexPersistenceMgr {
    private static final Logger LOG = LoggerFactory.getLogger(IndexPersistenceMgr.class);

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

    // use two separate cache for write and read
    final Cache<Long, CachedFileInfo> writeFileInfoCache;
    final Cache<Long, CachedFileInfo> readFileInfoCache;
    final FileInfoBackingCache fileInfoBackingCache;

    final int openFileLimit;
    final int pageSize;
    final int entriesPerPage;

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final SnapshotMap<Long, Boolean> activeLedgers;
    final LedgerDirsManager ledgerDirsManager;

    private final IndexPersistenceMgrStats persistenceMgrStats;

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

        // build the file info cache
        int concurrencyLevel = Math.max(1, Math.max(conf.getNumAddWorkerThreads(), conf.getNumReadWorkerThreads()));
        fileInfoBackingCache = new FileInfoBackingCache(this::createFileInfoBackingFile,
                conf.getFileInfoFormatVersionToWrite());
        RemovalListener<Long, CachedFileInfo> fileInfoEvictionListener = this::handleLedgerEviction;
        writeFileInfoCache = buildCache(
            concurrencyLevel,
            conf.getFileInfoCacheInitialCapacity(),
            openFileLimit,
            conf.getFileInfoMaxIdleTime(),
            fileInfoEvictionListener);
        readFileInfoCache = buildCache(
            concurrencyLevel,
            2 * conf.getFileInfoCacheInitialCapacity(),
            2 * openFileLimit,
            conf.getFileInfoMaxIdleTime(),
            fileInfoEvictionListener);

        // Expose Stats
        persistenceMgrStats = new IndexPersistenceMgrStats(
            statsLogger,
            () -> writeFileInfoCache.size(),
            () -> readFileInfoCache.size()
        );
    }

    private static Cache<Long, CachedFileInfo> buildCache(int concurrencyLevel,
                                                          int initialCapacity,
                                                          int maximumSize,
                                                          long expireAfterAccessSeconds,
                                                          RemovalListener<Long, CachedFileInfo> removalListener) {
        CacheBuilder<Long, CachedFileInfo> builder = CacheBuilder.newBuilder()
            .concurrencyLevel(concurrencyLevel)
            .initialCapacity(initialCapacity)
            .maximumSize(maximumSize)
            .removalListener(removalListener);
        if (expireAfterAccessSeconds > 0) {
            builder.expireAfterAccess(expireAfterAccessSeconds, TimeUnit.SECONDS);
        }
        return builder.build();
    }

    private File createFileInfoBackingFile(long ledger, boolean createIfMissing) throws IOException {
        File lf = findIndexFile(ledger);
        if (null == lf) {
            if (!createIfMissing) {
                throw new Bookie.NoLedgerException(ledger);
            }
            // We don't have a ledger index file on disk or in cache, so create it.
            lf = getNewLedgerIndexFile(ledger, null);
        }
        return lf;
    }

    /**
     * When a ledger is evicted, we need to make sure there's no other thread
     * trying to get FileInfo for that ledger at the same time when we close
     * the FileInfo.
     */
    private void handleLedgerEviction(RemovalNotification<Long, CachedFileInfo> notification) {
        CachedFileInfo fileInfo = notification.getValue();
        if (null == fileInfo || null == notification.getKey()) {
            return;
        }
        if (notification.wasEvicted()) {
            persistenceMgrStats.getEvictedLedgersCounter().inc();
        }
        fileInfo.release();
    }

    /**
     * Get the FileInfo and increase reference count.
     * When we get FileInfo from cache, we need to make sure it is synchronized
     * with eviction, otherwise there might be a race condition as we get
     * the FileInfo from cache, that FileInfo is then evicted and closed before we
     * could even increase the reference counter.
     */
    CachedFileInfo getFileInfo(final Long ledger, final byte[] masterKey) throws IOException {
        try {
            CachedFileInfo fi;
            persistenceMgrStats.getPendingGetFileInfoCounter().inc();
            Callable<CachedFileInfo> loader = () -> {
                CachedFileInfo fileInfo = fileInfoBackingCache.loadFileInfo(ledger, masterKey);
                activeLedgers.put(ledger, true);
                return fileInfo;
            };
            do {
                if (null != masterKey) {
                    fi = writeFileInfoCache.get(ledger, loader);
                } else {
                    fi = readFileInfoCache.get(ledger, loader);
                }
                if (!fi.tryRetain()) {
                    // defensively ensure that dead fileinfo objects don't exist in the
                    // cache. They shouldn't if refcounting is correct, but if someone
                    // does a double release, the fileinfo will be cleaned up, while
                    // remaining in the cache, which could cause a tight loop in this method.
                    boolean inWriteMap = writeFileInfoCache.asMap().remove(ledger, fi);
                    boolean inReadMap = readFileInfoCache.asMap().remove(ledger, fi);
                    if (inWriteMap || inReadMap) {
                        LOG.error("Dead fileinfo({}) forced out of cache (write:{}, read:{}). "
                                  + "It must have been double-released somewhere.",
                                  fi, inWriteMap, inReadMap);
                    }
                    fi = null;
                }
            } while (fi == null);

            return fi;
        } catch (ExecutionException | UncheckedExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
                throw (IOException) ee.getCause();
            } else {
                throw new LedgerCache.NoIndexForLedgerException("Failed to load file info for ledger " + ledger, ee);
            }
        } finally {
            persistenceMgrStats.getPendingGetFileInfoCounter().dec();
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
        File dir = ledgerDirsManager.pickRandomWritableDirForNewIndexFile(excludedDir);
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
    void removeLedger(Long ledgerId) throws IOException {
        // Delete the ledger's index file and close the FileInfo
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            // Don't force flush. There's no need since we're deleting the ledger
            // anyway, and recreating the file at this point, although safe, will
            // force the garbage collector to do more work later.
            fi.close(false);
            fi.delete();
        } finally {
            if (fi != null) {
                // should release use count
                fi.release();
                // Remove it from the active ledger manager
                activeLedgers.remove(ledgerId);
                // Now remove it from cache
                writeFileInfoCache.invalidate(ledgerId);
                readFileInfoCache.invalidate(ledgerId);
            }
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

    void close() throws IOException {
        // Don't force create the file. We may have many dirty ledgers and file create/flush
        // can be quite expensive as a result. We can use this optimization in this case
        // because metadata will be recovered from the journal when we restart anyway.
        fileInfoBackingCache.closeAllWithoutFlushing();
        writeFileInfoCache.invalidateAll();
        readFileInfoCache.invalidateAll();
    }

    Long getLastAddConfirmed(long ledgerId) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.getLastAddConfirmed();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                          long previousLAC,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.waitForLastAddConfirmedUpdate(previousLAC, watcher);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            fi.cancelWaitForLastAddConfirmedUpdate(watcher);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    long updateLastAddConfirmed(long ledgerId, long lac) throws IOException {
        CachedFileInfo fi = null;
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
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.getMasterKey();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, masterKey);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean setFenced(long ledgerId) throws IOException {
        CachedFileInfo fi = null;
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
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.isFenced();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            fi.setExplicitLac(lac);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    public ByteBuf getExplicitLac(long ledgerId) {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.getExplicitLac();
        } catch (IOException e) {
            LOG.error("Exception during getLastAddConfirmed", e);
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

    private void relocateIndexFileAndFlushHeader(long ledger, FileInfo fi) throws IOException {
        File currentDir = getLedgerDirForLedger(fi);
        if (ledgerDirsManager.isDirFull(currentDir)) {
            try {
                moveLedgerIndexFile(ledger, fi);
            } catch (NoWritableLedgerDirException nwe) {
                /*
                 * if there is no other indexDir, which could accommodate new
                 * indexFile but the current indexDir has enough space
                 * (minUsableSizeForIndexFileCreation) for this flushHeader
                 * operation, then it is ok to proceed without moving
                 * LedgerIndexFile.
                 */
                if (!ledgerDirsManager.isDirWritableForNewIndexFile(currentDir)) {
                    throw nwe;
                }
            }
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
        try {
            fi.moveToNewLocation(newLedgerIndexFile, fi.getSizeSinceLastwrite());
        } catch (FileInfo.FileInfoDeletedException fileInfoDeleted) {
            // File concurrently deleted
            throw new Bookie.NoLedgerException(l);
        }
    }

    void flushLedgerHeader(long ledger) throws IOException {
        CachedFileInfo fi = null;
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
        CachedFileInfo fi = null;
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
                              int start, int count) throws IOException, Bookie.NoLedgerException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Writing {} buffers of {}", count, Long.toHexString(ledger));
        }
        if (count == 0) {
            return;
        }
        ByteBuffer[] buffs = new ByteBuffer[count];
        for (int j = 0; j < count; j++) {
            buffs[j] = entries.get(start + j).getPageToWrite();
            if (entries.get(start + j).getLedger() != ledger) {
                throw new IOException("Writing to " + ledger + " but page belongs to "
                                + entries.get(start + j).getLedger());
            }
        }
        long totalWritten = 0;
        while (buffs[buffs.length - 1].remaining() > 0) {
            long rc = 0;
            try {
                rc = fi.write(buffs, entries.get(start + 0).getFirstEntryPosition());
            } catch (FileInfo.FileInfoDeletedException e) {
                throw new Bookie.NoLedgerException(ledger);
            }
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

    /**
     * Update the ledger entry page.
     *
     * @param lep
     *          ledger entry page
     * @return true if it is a new page, otherwise false.
     * @throws IOException
     */
    boolean updatePage(LedgerEntryPage lep) throws IOException {
        if (!lep.isClean()) {
            throw new IOException("Trying to update a dirty page");
        }
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(lep.getLedger(), null);
            long pos = lep.getFirstEntryPosition();
            if (pos >= fi.size()) {
                lep.zeroPage();
                return true;
            } else {
                lep.readPage(fi);
                return false;
            }
        } finally {
            if (fi != null) {
                fi.release();
            }
        }
    }

    long getPersistEntryBeyondInMem(long ledgerId, long lastEntryInMem) throws IOException {
        CachedFileInfo fi = null;
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
                // we read the last page from file size minus page size, so it should not encounter short read
                // exception. if it does, it is an unexpected situation, then throw the exception and fail it
                // immediately.
                try {
                    fi.read(bb, position, false);
                } catch (ShortReadException sre) {
                    // throw a more meaningful exception with ledger id
                    throw new ShortReadException("Short read on ledger " + ledgerId + " : ", sre);
                }
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

    /**
     * Read ledger meta.
     * @param ledgerId Ledger Id
     */
    public LedgerCache.LedgerIndexMetadata readLedgerIndexMetadata(long ledgerId) throws IOException {
        CachedFileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return new LedgerCache.LedgerIndexMetadata(
                    fi.getMasterKey(),
                    fi.size(),
                    fi.isFenced());
        } finally {
            if (fi != null) {
                fi.release();
            }
        }
    }
}
