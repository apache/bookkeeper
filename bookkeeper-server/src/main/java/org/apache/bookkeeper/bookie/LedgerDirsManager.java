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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_WRITABLE_DIRS;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages ledger directories used by the bookie.
 */
public class LedgerDirsManager {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerDirsManager.class);

    private volatile List<File> filledDirs;
    private final List<File> ledgerDirectories;
    private volatile List<File> writableLedgerDirectories;
    private final List<LedgerDirsListener> listeners;
    private final Random rand = new Random();
    private final ConcurrentMap<File, Float> diskUsages =
            new ConcurrentHashMap<File, Float>();
    private final long entryLogSize;
    private long minUsableSizeForEntryLogCreation;
    private long minUsableSizeForIndexFileCreation;

    private final DiskChecker diskChecker;

    public LedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker) {
        this(conf, dirs, diskChecker, NullStatsLogger.INSTANCE);
    }

    public LedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker, StatsLogger statsLogger) {
        this.ledgerDirectories = Arrays.asList(Bookie
                .getCurrentDirectories(dirs));
        this.writableLedgerDirectories = new ArrayList<File>(ledgerDirectories);
        this.filledDirs = new ArrayList<File>();
        this.listeners = new ArrayList<LedgerDirsListener>();
        this.entryLogSize = conf.getEntryLogSizeLimit();
        this.minUsableSizeForIndexFileCreation = conf.getMinUsableSizeForIndexFileCreation();
        this.minUsableSizeForEntryLogCreation = conf.getMinUsableSizeForEntryLogCreation();
        for (File dir : ledgerDirectories) {
            diskUsages.put(dir, 0f);
            String statName = "dir_" + dir.getParent().replace('/', '_') + "_usage";
            final File targetDir = dir;
            statsLogger.registerGauge(statName, new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return diskUsages.get(targetDir) * 100;
                }
            });
        }

        this.diskChecker = diskChecker;
        statsLogger.registerGauge(LD_WRITABLE_DIRS, new Gauge<Number>() {

            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return writableLedgerDirectories.size();
            }
        });
    }

    /**
     * Get all ledger dirs configured.
     */
    public List<File> getAllLedgerDirs() {
        return ledgerDirectories;
    }

    /**
     * Get all dir listeners.
     *
     * @return list of listeners
     */
    public List<LedgerDirsListener> getListeners() {
        return listeners;
    }

    /**
     * Calculate the total amount of free space available in all of the ledger directories put together.
     *
     * @return totalDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalFreeSpace(List<File> dirs) throws IOException {
        return diskChecker.getTotalFreeSpace(dirs);
    }

    /**
     * Calculate the total amount of free space available in all of the ledger directories put together.
     *
     * @return freeDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalDiskSpace(List<File> dirs) throws IOException {
        return diskChecker.getTotalDiskSpace(dirs);
    }

    /**
     * Get disk usages map.
     *
     * @return disk usages map
     */
    public ConcurrentMap<File, Float> getDiskUsages() {
        return diskUsages;
    }

    /**
     * Get only writable ledger dirs.
     */
    public List<File> getWritableLedgerDirs()
            throws NoWritableLedgerDirException {
        if (writableLedgerDirectories.isEmpty()) {
            String errMsg = "All ledger directories are non writable";
            NoWritableLedgerDirException e = new NoWritableLedgerDirException(
                    errMsg);
            throw e;
        }
        return writableLedgerDirectories;
    }

    /**
     * @return true if the writableLedgerDirs list has entries
     */
    public boolean hasWritableLedgerDirs() {
        return !writableLedgerDirectories.isEmpty();
    }

    public List<File> getWritableLedgerDirsForNewLog() throws NoWritableLedgerDirException {
        if (!writableLedgerDirectories.isEmpty()) {
            return writableLedgerDirectories;
        }

        // We don't have writable Ledger Dirs. But we are still okay to create new entry log files if we have enough
        // disk spaces. This allows bookie can still function at readonly mode. Because compaction, journal replays
        // can still write data to disks.
        return getDirsAboveUsableThresholdSize(minUsableSizeForEntryLogCreation, true);
    }

    List<File> getDirsAboveUsableThresholdSize(long thresholdSize, boolean loggingNoWritable)
            throws NoWritableLedgerDirException {
        List<File> fullLedgerDirsToAccomodate = new ArrayList<File>();
        for (File dir: this.ledgerDirectories) {
            // Pick dirs which can accommodate little more than thresholdSize
            if (dir.getUsableSpace() > thresholdSize) {
                fullLedgerDirsToAccomodate.add(dir);
            }
        }

        if (!fullLedgerDirsToAccomodate.isEmpty()) {
            if (loggingNoWritable) {
                LOG.info("No writable ledger dirs below diskUsageThreshold. "
                    + "But Dirs that can accommodate {} are: {}", thresholdSize, fullLedgerDirsToAccomodate);
            }
            return fullLedgerDirsToAccomodate;
        }

        // We will reach here when we find no ledgerDir which has atleast
        // thresholdSize usable space
        String errMsg = "All ledger directories are non writable and no reserved space (" + thresholdSize + ") left.";
        NoWritableLedgerDirException e = new NoWritableLedgerDirException(errMsg);
        if (loggingNoWritable) {
            LOG.error(errMsg, e);
        }
        throw e;
    }

    /**
     * @return full-filled ledger dirs.
     */
    public List<File> getFullFilledLedgerDirs() {
        return filledDirs;
    }

    /**
     * Get dirs, which are full more than threshold.
     */
    public boolean isDirFull(File dir) {
        return filledDirs.contains(dir);
    }

    /**
     * Add the dir to filled dirs list.
     */
    @VisibleForTesting
    public void addToFilledDirs(File dir) {
        if (!filledDirs.contains(dir)) {
            LOG.warn(dir + " is out of space. Adding it to filled dirs list");
            // Update filled dirs list
            List<File> updatedFilledDirs = new ArrayList<File>(filledDirs);
            updatedFilledDirs.add(dir);
            filledDirs = updatedFilledDirs;
            // Update the writable ledgers list
            List<File> newDirs = new ArrayList<File>(writableLedgerDirectories);
            newDirs.removeAll(filledDirs);
            writableLedgerDirectories = newDirs;
            // Notify listeners about disk full
            for (LedgerDirsListener listener : listeners) {
                listener.diskFull(dir);
            }
        }
    }

    /**
     * Add the dir to writable dirs list.
     *
     * @param dir Dir
     */
    public void addToWritableDirs(File dir, boolean underWarnThreshold) {
        if (writableLedgerDirectories.contains(dir)) {
            return;
        }
        LOG.info("{} becomes writable. Adding it to writable dirs list.", dir);
        // Update writable dirs list
        List<File> updatedWritableDirs = new ArrayList<File>(writableLedgerDirectories);
        updatedWritableDirs.add(dir);
        writableLedgerDirectories = updatedWritableDirs;
        // Update the filled dirs list
        List<File> newDirs = new ArrayList<File>(filledDirs);
        newDirs.removeAll(writableLedgerDirectories);
        filledDirs = newDirs;
        // Notify listeners about disk writable
        for (LedgerDirsListener listener : listeners) {
            if (underWarnThreshold) {
                listener.diskWritable(dir);
            } else {
                listener.diskJustWritable(dir);
            }
        }
    }

    /**
     * Returns one of the ledger dir from writable dirs list randomly.
     */
    File pickRandomWritableDir() throws NoWritableLedgerDirException {
        return pickRandomWritableDir(null);
    }

    /**
     * Pick up a writable dir from available dirs list randomly. The <code>excludedDir</code>
     * will not be pickedup.
     *
     * @param excludedDir
     *          The directory to exclude during pickup.
     * @throws NoWritableLedgerDirException if there is no writable dir available.
     */
    File pickRandomWritableDir(File excludedDir) throws NoWritableLedgerDirException {
        List<File> writableDirs = getWritableLedgerDirs();
        return pickRandomDir(writableDirs, excludedDir);
    }

    /**
     * Pick up a dir randomly from writableLedgerDirectories. If writableLedgerDirectories is empty
     * then pick up a dir randomly from the ledger/indexdirs which have usable space more than
     * minUsableSizeForIndexFileCreation.
     *
     * @param excludedDir The directory to exclude during pickup.
     * @return
     * @throws NoWritableLedgerDirException if there is no dir available.
     */
    File pickRandomWritableDirForNewIndexFile(File excludedDir) throws NoWritableLedgerDirException {
        final List<File> writableDirsForNewIndexFile;
        if (!writableLedgerDirectories.isEmpty()) {
            writableDirsForNewIndexFile = writableLedgerDirectories;
        } else {
            // We don't have writable Index Dirs.
            // That means we must have turned readonly. But
            // during the Bookie restart, while replaying the journal there might be a need
            // to create new Index file and it should proceed.
            writableDirsForNewIndexFile = getDirsAboveUsableThresholdSize(minUsableSizeForIndexFileCreation, true);
        }
        return pickRandomDir(writableDirsForNewIndexFile, excludedDir);
    }

    boolean isDirWritableForNewIndexFile(File indexDir) {
        return (ledgerDirectories.contains(indexDir)
                && (indexDir.getUsableSpace() > minUsableSizeForIndexFileCreation));
    }

    /**
     * Return one dir from all dirs, regardless writable or not.
     */
    File pickRandomDir(File excludedDir) throws NoWritableLedgerDirException {
        return pickRandomDir(getAllLedgerDirs(), excludedDir);
    }

    File pickRandomDir(List<File> dirs, File excludedDir) throws NoWritableLedgerDirException {
        final int start = rand.nextInt(dirs.size());
        int idx = start;
        File candidate = dirs.get(idx);
        while (null != excludedDir && excludedDir.equals(candidate)) {
            idx = (idx + 1) % dirs.size();
            if (idx == start) {
                // after searching all available dirs,
                // no writable dir is found
                throw new NoWritableLedgerDirException("No writable directories found from "
                        + " available writable dirs (" + dirs + ") : exclude dir "
                        + excludedDir);
            }
            candidate = dirs.get(idx);
        }
        return candidate;
    }

    public void addLedgerDirsListener(LedgerDirsListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public DiskChecker getDiskChecker() {
        return diskChecker;
    }

    /**
     * Indicates All configured ledger directories are full.
     */
    public static class NoWritableLedgerDirException extends IOException {
        private static final long serialVersionUID = -8696901285061448421L;

        public NoWritableLedgerDirException(String errMsg) {
            super(errMsg);
        }
    }

    /**
     * Listener for the disk check events will be notified from the
     * {@link LedgerDirsManager} whenever disk full/failure detected.
     */
    public interface LedgerDirsListener {
        /**
         * This will be notified on disk failure/disk error.
         *
         * @param disk Failed disk
         */
        default void diskFailed(File disk) {}

        /**
         * Notified when the disk usage warn threshold is exceeded on the drive.
         * @param disk
         */
        default void diskAlmostFull(File disk) {}

        /**
         * This will be notified on disk detected as full.
         *
         * @param disk Filled disk
         */
        default void diskFull(File disk) {}

        /**
         * This will be notified on disk detected as writable and under warn threshold.
         *
         * @param disk Writable disk
         */
        default void diskWritable(File disk) {}

        /**
         * This will be notified on disk detected as writable but still in warn threshold.
         *
         * @param disk Writable disk
         */
        default void diskJustWritable(File disk) {}

        /**
         * This will be notified whenever all disks are detected as full.
         *
         * <p>Normal writes will be rejected when disks are detected as "full". High priority writes
         * such as ledger recovery writes can go through if disks are still available.
         *
         * @param highPriorityWritesAllowed the parameter indicates we are still have disk spaces for high priority
         *                                  writes even disks are detected as "full"
         */
        default void allDisksFull(boolean highPriorityWritesAllowed) {}

        /**
         * This will notify the fatal errors.
         */
        default void fatalError() {}
    }
}
