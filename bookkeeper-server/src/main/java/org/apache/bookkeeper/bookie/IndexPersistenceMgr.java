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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LEDGER_CACHE_NUM_EVICTED_LEDGERS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.PENDING_GET_FILE_INFO;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_FILE_INFO_CACHE_SIZE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_FILE_INFO_CACHE_SIZE;

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
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
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
    final Cache<Long, FileInfo> writeFileInfoCache;
    final Cache<Long, FileInfo> readFileInfoCache;
    final int openFileLimit;
    final int pageSize;
    final int entriesPerPage;
    // Lock
    final ReentrantReadWriteLock fileInfoLock = new ReentrantReadWriteLock();
    // ThreadPool
    final ScheduledExecutorService evictionThreadPool = Executors.newSingleThreadScheduledExecutor();

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final SnapshotMap<Long, Boolean> activeLedgers;
    final LedgerDirsManager ledgerDirsManager;

    // Stats
    private final Counter evictedLedgersCounter;
    private final Counter pendingGetFileInfoCounter;

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

        // build the file info cache
        int concurrencyLevel = Math.max(1, Math.max(conf.getNumAddWorkerThreads(), conf.getNumReadWorkerThreads()));
        RemovalListener<Long, FileInfo> fileInfoEvictionListener = this::handleLedgerEviction;
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
        evictedLedgersCounter = statsLogger.getCounter(LEDGER_CACHE_NUM_EVICTED_LEDGERS);
        pendingGetFileInfoCounter = statsLogger.getCounter(PENDING_GET_FILE_INFO);
        statsLogger.registerGauge(WRITE_FILE_INFO_CACHE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return writeFileInfoCache.size();
            }
        });
        statsLogger.registerGauge(READ_FILE_INFO_CACHE_SIZE, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readFileInfoCache.size();
            }
        });
    }

    private static Cache<Long, FileInfo> buildCache(int concurrencyLevel,
                                            int initialCapacity,
                                            int maximumSize,
                                            long expireAfterAccessSeconds,
                                            RemovalListener<Long, FileInfo> removalListener) {
        CacheBuilder<Long, FileInfo> builder = CacheBuilder.newBuilder()
            .concurrencyLevel(concurrencyLevel)
            .initialCapacity(initialCapacity)
            .maximumSize(maximumSize)
            .removalListener(removalListener);
        if (expireAfterAccessSeconds > 0) {
            builder.expireAfterAccess(expireAfterAccessSeconds, TimeUnit.SECONDS);
        }
        return builder.build();
    }

    /**
     * When a ledger is evicted, we need to make sure there's no other thread
     * trying to get FileInfo for that ledger at the same time when we close
     * the FileInfo.
     */
    private void handleLedgerEviction(RemovalNotification<Long, FileInfo> notification) {
        FileInfo fileInfo = notification.getValue();
        Long ledgerId = notification.getKey();
        if (null == fileInfo || null == notification.getKey()) {
            return;
        }
        if (notification.wasEvicted()) {
            evictedLedgersCounter.inc();
            // we need to acquire the write lock in another thread,
            // otherwise there could be dead lock happening.
            evictionThreadPool.execute(() -> {
                fileInfoLock.writeLock().lock();
                try {
                    // We only close the fileInfo when we evict the FileInfo from both cache
                    if (!readFileInfoCache.asMap().containsKey(ledgerId)
                            && !writeFileInfoCache.asMap().containsKey(ledgerId)) {
                        fileInfo.close(true);
                    }
                } catch (IOException e) {
                    LOG.error("Exception closing file info when ledger {} is evicted from file info cache.",
                        ledgerId, e);
                } finally {
                    fileInfoLock.writeLock().unlock();
                }
            });
        }
        fileInfo.release();
    }

    class FileInfoLoader implements Callable<FileInfo> {

        final long ledger;
        final byte[] masterKey;

        FileInfoLoader(long ledger, byte[] masterKey) {
            this.ledger = ledger;
            this.masterKey = masterKey;
        }

        @Override
        public FileInfo call() throws IOException {
            File lf = findIndexFile(ledger);
            if (null == lf) {
                if (null == masterKey) {
                    throw new Bookie.NoLedgerException(ledger);
                }
                // We don't have a ledger index file on disk or in cache, so create it.
                lf = getNewLedgerIndexFile(ledger, null);
                activeLedgers.put(ledger, true);
            }
            FileInfo fi = new FileInfo(lf, masterKey);
            fi.use();
            return fi;
        }
    }

    /**
     * Get the FileInfo and increase reference count.
     * When we get FileInfo from cache, we need to make sure it is synchronized
     * with eviction, otherwise there might be a race condition as we get
     * the FileInfo from cache, that FileInfo is then evicted and closed before we
     * could even increase the reference counter.
     */
    FileInfo getFileInfo(final Long ledger, final byte masterKey[]) throws IOException {
        try {
            FileInfo fi;
            pendingGetFileInfoCounter.inc();
            fileInfoLock.readLock().lock();
            if (null != masterKey) {
                fi = writeFileInfoCache.get(ledger,
                    new FileInfoLoader(ledger, masterKey));
                if (null == readFileInfoCache.asMap().putIfAbsent(ledger, fi)) {
                    fi.use();
                }
            } else {
                fi = readFileInfoCache.get(ledger,
                    new FileInfoLoader(ledger, null));
            }
            fi.use();
            return fi;
        } catch (ExecutionException | UncheckedExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
                throw (IOException) ee.getCause();
            } else {
                throw new IOException("Failed to load file info for ledger " + ledger, ee);
            }
        } finally {
            pendingGetFileInfoCounter.dec();
            fileInfoLock.readLock().unlock();
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
        FileInfo fi = null;
        fileInfoLock.writeLock().lock();
        try {
            fi = getFileInfo(ledgerId, null);
            // Don't force flush. There's no need since we're deleting the ledger
            // anyway, and recreating the file at this point, although safe, will
            // force the garbage collector to do more work later.
            fi.close(false);
            fi.delete();
        } finally {
            try {
                if (fi != null) {
                    // should release use count
                    fi.release();
                    // Remove it from the active ledger manager
                    activeLedgers.remove(ledgerId);
                    // Now remove it from cache
                    writeFileInfoCache.invalidate(ledgerId);
                    readFileInfoCache.invalidate(ledgerId);
                }
            } finally {
                fileInfoLock.writeLock().unlock();
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
        try {
            fileInfoLock.writeLock().lock();
            for (Map.Entry<Long, FileInfo> entry : writeFileInfoCache.asMap().entrySet()) {
                entry.getValue().close(false);
            }
            for (Map.Entry<Long, FileInfo> entry : readFileInfoCache.asMap().entrySet()) {
                entry.getValue().close(false);
            }
            writeFileInfoCache.invalidateAll();
            readFileInfoCache.invalidateAll();
        } finally {
            fileInfoLock.writeLock().unlock();
        }
        evictionThreadPool.shutdown();
        try {
            evictionThreadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            //ignore
        }
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

    Observable waitForLastAddConfirmedUpdate(long ledgerId, long previoisLAC, Observer observer) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            return fi.waitForLastAddConfirmedUpdate(previoisLAC, observer);
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
        FileInfo fi = null;
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

    void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
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

    public ByteBuf getExplicitLac(long ledgerId) {
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
        FileInfo fi = null;
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

}
