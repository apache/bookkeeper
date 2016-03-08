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
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.bookkeeper.util.DiskChecker.DiskWarnThresholdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_WRITABLE_DIRS;

/**
 * This class manages ledger directories used by the bookie.
 */
public class LedgerDirsManager {
    private final static Logger LOG = LoggerFactory
            .getLogger(LedgerDirsManager.class);

    private volatile List<File> filledDirs;
    private final List<File> ledgerDirectories;
    private volatile List<File> writableLedgerDirectories;
    private final DiskChecker diskChecker;
    private final List<LedgerDirsListener> listeners;
    private final LedgerDirsMonitor monitor;
    private final Random rand = new Random();
    private final ConcurrentMap<File, Float> diskUsages =
            new ConcurrentHashMap<File, Float>();

    public LedgerDirsManager(ServerConfiguration conf, File[] dirs) {
        this(conf, dirs, NullStatsLogger.INSTANCE);
    }

    LedgerDirsManager(ServerConfiguration conf, File[] dirs, StatsLogger statsLogger) {
        this(conf, dirs, statsLogger, new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
    }

    @VisibleForTesting
    LedgerDirsManager(ServerConfiguration conf, File[] dirs, StatsLogger statsLogger, DiskChecker diskChecker) {
        this.ledgerDirectories = Arrays.asList(Bookie
                .getCurrentDirectories(dirs));
        this.writableLedgerDirectories = new ArrayList<File>(ledgerDirectories);
        this.filledDirs = new ArrayList<File>();
        this.listeners = new ArrayList<LedgerDirsListener>();
        this.diskChecker = diskChecker;
        this.monitor = new LedgerDirsMonitor(conf.getDiskCheckInterval());
        for (File dir : dirs) {
            diskUsages.put(dir, 0f);
            String statName = "dir_" + dir.getPath().replace('/', '_') + "_usage";
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
     * Get all ledger dirs configured
     */
    public List<File> getAllLedgerDirs() {
        return ledgerDirectories;
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
            LOG.error(errMsg, e);
            throw e;
        }
        return writableLedgerDirectories;
    }

    /**
     * @return full-filled ledger dirs.
     */
    public List<File> getFullFilledLedgerDirs() {
        return filledDirs;
    }

    /**
     * Get dirs, which are full more than threshold
     */
    public boolean isDirFull(File dir) {
        return filledDirs.contains(dir);
    }

    /**
     * Add the dir to filled dirs list
     */
    @VisibleForTesting
    public void addToFilledDirs(File dir) {
        if (!filledDirs.contains(dir)) {
            LOG.warn(dir + " is out of space."
                    + " Adding it to filled dirs list");
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

        final int start = rand.nextInt(writableDirs.size());
        int idx = start;
        File candidate = writableDirs.get(idx);
        while (null != excludedDir && excludedDir.equals(candidate)) {
            idx = (idx + 1) % writableDirs.size();
            if (idx == start) {
                // after searching all available dirs,
                // no writable dir is found
                throw new NoWritableLedgerDirException("No writable directories found from "
                        + " available writable dirs (" + writableDirs + ") : exclude dir "
                        + excludedDir);
            }
            candidate = writableDirs.get(idx);
        }
        return candidate;
    }

    public void addLedgerDirsListener(LedgerDirsListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * Sweep through all the directories to check disk errors or disk full.
     *
     * @throws DiskErrorException
     *             If disk having errors
     * @throws NoWritableLedgerDirException
     *             If all the configured ledger directories are full or having
     *             less space than threshold
     */
    public void init() throws DiskErrorException, NoWritableLedgerDirException {
        monitor.checkDirs(writableLedgerDirectories);
    }

    // start the daemon for disk monitoring
    public void start() {
        monitor.setDaemon(true);
        monitor.start();
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
        LOG.info("Shutting down LedgerDirsMonitor");
        monitor.interrupt();
        try {
            monitor.join();
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    /**
     * Thread to monitor the disk space periodically.
     */
    private class LedgerDirsMonitor extends BookieThread {
        private final int interval;

        public LedgerDirsMonitor(int interval) {
            super("LedgerDirsMonitorThread");
            this.interval = interval;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    List<File> writableDirs = getWritableLedgerDirs();
                    // Check all writable dirs disk space usage.
                    for (File dir : writableDirs) {
                        try {
                            diskUsages.put(dir, diskChecker.checkDir(dir));
                        } catch (DiskErrorException e) {
                            LOG.error("Ledger directory {} failed on disk checking : ", dir, e);
                            // Notify disk failure to all listeners
                            for (LedgerDirsListener listener : listeners) {
                                listener.diskFailed(dir);
                            }
                        } catch (DiskWarnThresholdException e) {
                            LOG.warn("Ledger directory {} is almost full.", dir);
                            diskUsages.put(dir, e.getUsage());
                            for (LedgerDirsListener listener : listeners) {
                                listener.diskAlmostFull(dir);
                            }
                        } catch (DiskOutOfSpaceException e) {
                            LOG.error("Ledger directory {} is out-of-space.", dir);
                            diskUsages.put(dir, e.getUsage());
                            // Notify disk full to all listeners
                            addToFilledDirs(dir);
                        }
                    }
                } catch (NoWritableLedgerDirException e) {
                    for (LedgerDirsListener listener : listeners) {
                        listener.allDisksFull();
                    }
                }

                List<File> fullfilledDirs = new ArrayList<File>(getFullFilledLedgerDirs());
                // Check all full-filled disk space usage
                for (File dir : fullfilledDirs) {
                    try {
                        diskUsages.put(dir, diskChecker.checkDir(dir));
                        addToWritableDirs(dir, true);
                    } catch (DiskErrorException e) {
                        // Notify disk failure to all the listeners
                        for (LedgerDirsListener listener : listeners) {
                            listener.diskFailed(dir);
                        }
                    } catch (DiskWarnThresholdException e) {
                        diskUsages.put(dir, e.getUsage());
                        // the full-filled dir become writable but still above warn threshold
                        addToWritableDirs(dir, false);
                    } catch (DiskOutOfSpaceException e) {
                        // the full-filled dir is still full-filled
                        diskUsages.put(dir, e.getUsage());
                    }
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    LOG.info("LedgerDirsMonitor thread is interrupted");
                    break;
                }
            }
            LOG.info("LedgerDirsMonitorThread exited!");
        }

        private void checkDirs(List<File> writableDirs)
                throws DiskErrorException, NoWritableLedgerDirException {
            for (File dir : writableDirs) {
                try {
                    diskChecker.checkDir(dir);
                } catch (DiskWarnThresholdException e) {
                    // nop
                } catch (DiskOutOfSpaceException e) {
                    addToFilledDirs(dir);
                }
            }
            getWritableLedgerDirs();
        }
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
    public static interface LedgerDirsListener {
        /**
         * This will be notified on disk failure/disk error
         *
         * @param disk
         *            Failed disk
         */
        void diskFailed(File disk);

        /**
         * Notified when the disk usage warn threshold is exceeded on
         * the drive.
         * @param disk
         */
        void diskAlmostFull(File disk);

        /**
         * This will be notified on disk detected as full
         *
         * @param disk
         *            Filled disk
         */
        void diskFull(File disk);

        /**
         * This will be notified on disk detected as writable and under warn threshold
         *
         * @param disk
         *          Writable disk
         */
        void diskWritable(File disk);

        /**
         * This will be notified on disk detected as writable but still in warn threshold
         *
         * @param disk
         *          Writable disk
         */
        void diskJustWritable(File disk);

        /**
         * This will be notified whenever all disks are detected as full.
         */
        void allDisksFull();

        /**
         * This will notify the fatal errors.
         */
        void fatalError();
    }
}
