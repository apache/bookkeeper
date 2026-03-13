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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.bookkeeper.util.DiskChecker.DiskWarnThresholdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread to monitor the disk space periodically.
 */
class LedgerDirsMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerDirsMonitor.class);

    private final int interval;
    private final ServerConfiguration conf;
    private final DiskChecker diskChecker;
    private final List<LedgerDirsManager> dirsManagers;
    private final long minUsableSizeForHighPriorityWrites;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checkTask;
    private boolean isFirstLoopOfCheckTask = true;

    public LedgerDirsMonitor(final ServerConfiguration conf,
                             final DiskChecker diskChecker,
                             final List<LedgerDirsManager> dirsManagers) {
        validateThreshold(conf.getDiskUsageThreshold(), conf.getDiskLowWaterMarkUsageThreshold());
        this.interval = conf.getDiskCheckInterval();
        this.minUsableSizeForHighPriorityWrites = conf.getMinUsableSizeForHighPriorityWrites();
        this.conf = conf;
        this.diskChecker = diskChecker;
        this.dirsManagers = dirsManagers;
    }

    private void check(final LedgerDirsManager ldm) {
        final boolean isFirstLoopOfCheckTaskLocalValue = this.isFirstLoopOfCheckTask;
        if (isFirstLoopOfCheckTaskLocalValue) {
            this.isFirstLoopOfCheckTask = false;
        }
        final ConcurrentMap<File, Float> diskUsages = ldm.getDiskUsages();
        boolean someDiskFulled = false;
        boolean highPriorityWritesAllowed = true;
        boolean someDiskRecovered = false;

        try {
            List<File> writableDirs = ldm.getWritableLedgerDirs();
            // Check all writable dirs disk space usage.
            for (File dir : writableDirs) {
                try {
                    diskUsages.put(dir, diskChecker.checkDir(dir));
                } catch (DiskErrorException e) {
                    LOG.error("Ledger directory {} failed on disk checking : ", dir, e);
                    // Notify disk failure to all listeners
                    for (LedgerDirsListener listener : ldm.getListeners()) {
                        listener.diskFailed(dir);
                    }
                } catch (DiskWarnThresholdException e) {
                    diskUsages.compute(dir, (d, prevUsage) -> {
                        if (null == prevUsage || e.getUsage() != prevUsage) {
                            LOG.warn("Ledger directory {} is almost full : usage {}", dir, e.getUsage());
                        }
                        return e.getUsage();
                    });
                    ldm.addToWarnDirs(dir);
                } catch (DiskOutOfSpaceException e) {
                    diskUsages.compute(dir, (d, prevUsage) -> {
                        if (null == prevUsage || e.getUsage() != prevUsage) {
                            LOG.error("Ledger directory {} is out-of-space : usage {}", dir, e.getUsage());
                        }
                        return e.getUsage();
                    });
                    // Notify disk full to all listeners
                    ldm.addToFilledDirs(dir);
                    someDiskFulled = true;
                }
            }
            // Let's get NoWritableLedgerDirException without waiting for the next iteration
            // in case we are out of writable dirs
            // otherwise for the duration of {interval} we end up in the state where
            // bookie cannot get writable dir but considered to be writable
            ldm.getWritableLedgerDirs();
        } catch (NoWritableLedgerDirException e) {
            LOG.warn("LedgerDirsMonitor check process: All ledger directories are non writable");
            try {
                // disk check can be frequent, so disable 'loggingNoWritable' to avoid log flooding.
                ldm.getDirsAboveUsableThresholdSize(minUsableSizeForHighPriorityWrites, false);
            } catch (NoWritableLedgerDirException e1) {
                highPriorityWritesAllowed = false;
            }
            for (LedgerDirsListener listener : ldm.getListeners()) {
                listener.allDisksFull(highPriorityWritesAllowed);
            }
        }

        // - Update warnThresholdDirs disk usage.
        List<File> warnDirs = new ArrayList<File>(ldm.getWarnLedgerDirs());
        for (File dir : warnDirs) {
            try {
                diskUsages.put(dir, diskChecker.checkDir(dir));
                ldm.removeFromWarnDirs(dir);
            } catch (DiskErrorException e) {
                // Notify disk failure to all the listeners
                for (LedgerDirsListener listener : ldm.getListeners()) {
                    listener.diskFailed(dir);
                }
            } catch (DiskWarnThresholdException e) {
                diskUsages.put(dir, e.getUsage());
            } catch (DiskOutOfSpaceException e) {
                diskUsages.put(dir, e.getUsage());
                ldm.addToFilledDirs(dir);
            }
        }

        List<File> fulfilledDirs = new ArrayList<File>(ldm.getFullFilledLedgerDirs());
        boolean makeWritable = ldm.hasWritableLedgerDirs();

        // When bookie is in READONLY mode, i.e there are no writableLedgerDirs:
        // - Update fulfilledDirs disk usage.
        // - If the total disk usage is below DiskLowWaterMarkUsageThreshold
        // add fulfilledDirs back to writableLedgerDirs list if their usage is < conf.getDiskUsageThreshold.
        try {
            if (!makeWritable) {
                float totalDiskUsage = diskChecker.getTotalDiskUsage(ldm.getAllLedgerDirs());
                if (totalDiskUsage < conf.getDiskLowWaterMarkUsageThreshold()) {
                    makeWritable = true;
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Current TotalDiskUsage: {} is greater than LWMThreshold: {}."
                                    + " So not adding any filledDir to WritableDirsList",
                            totalDiskUsage, conf.getDiskLowWaterMarkUsageThreshold());
                }
            }
            // Update all full-filled disk space usage
            for (File dir : fulfilledDirs) {
                try {
                    diskUsages.put(dir, diskChecker.checkDir(dir));
                    if (makeWritable) {
                        ldm.addToWritableDirs(dir, true);
                    }
                    someDiskRecovered = true;
                } catch (DiskErrorException e) {
                    // Notify disk failure to all the listeners
                    for (LedgerDirsListener listener : ldm.getListeners()) {
                        listener.diskFailed(dir);
                    }
                } catch (DiskWarnThresholdException e) {
                    diskUsages.put(dir, e.getUsage());
                    // the full-filled dir become writable but still above the warn threshold
                    if (makeWritable) {
                        ldm.addToWritableDirs(dir, false);
                    }
                    someDiskRecovered = true;
                } catch (DiskOutOfSpaceException e) {
                    // the full-filled dir is still full-filled
                    diskUsages.put(dir, e.getUsage());
                }
            }
        } catch (IOException ioe) {
            LOG.error("Got IOException while monitoring Dirs", ioe);
            for (LedgerDirsListener listener : ldm.getListeners()) {
                listener.fatalError();
            }
        }

        if (isFirstLoopOfCheckTaskLocalValue && ldm.getFullFilledLedgerDirs().isEmpty()) {
            // notify no disk full.
            for (LedgerDirsListener listener : ldm.getListeners()) {
                listener.allDisksWritable();
            }
            return;
        }

        if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
            if (someDiskFulled && !ldm.getFullFilledLedgerDirs().isEmpty()) {
                // notify any disk full.
                for (LedgerDirsListener listener : ldm.getListeners()) {
                    listener.anyDiskFull(highPriorityWritesAllowed);
                }
            }

            if (someDiskRecovered && ldm.getFullFilledLedgerDirs().isEmpty()) {
                // notify all disk recovered.
                for (LedgerDirsListener listener : ldm.getListeners()) {
                    listener.allDisksWritable();
                }
            }
        }
    }

    @VisibleForTesting
    void check() {
        dirsManagers.forEach(this::check);
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
        checkDirs();
    }

    // start the daemon for disk monitoring
    public void start() {
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("LedgerDirsMonitorThread")
                .setDaemon(true)
                .build());
        this.checkTask = this.executor.scheduleAtFixedRate(this::check, interval, interval, TimeUnit.MILLISECONDS);
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
        LOG.info("Shutting down LedgerDirsMonitor");
        if (null != checkTask) {
            if (checkTask.cancel(true) && LOG.isDebugEnabled()) {
                LOG.debug("Failed to cancel check task in LedgerDirsMonitor");
            }
        }
        if (null != executor) {
            executor.shutdown();
        }
    }

    private void checkDirs() throws NoWritableLedgerDirException, DiskErrorException {
        for (LedgerDirsManager dirsManager : dirsManagers) {
            checkDirs(dirsManager);
        }
    }

    private void checkDirs(final LedgerDirsManager ldm)
            throws DiskErrorException, NoWritableLedgerDirException {
        for (File dir : ldm.getWritableLedgerDirs()) {
            try {
                diskChecker.checkDir(dir);
            } catch (DiskWarnThresholdException e) {
                // noop
            } catch (DiskOutOfSpaceException e) {
                ldm.addToFilledDirs(dir);
            }
        }
        ldm.getWritableLedgerDirs();
    }

    private void validateThreshold(float diskSpaceThreshold, float diskSpaceLwmThreshold) {
        if (diskSpaceThreshold <= 0 || diskSpaceThreshold >= 1 || diskSpaceLwmThreshold - diskSpaceThreshold > 1e-6) {
            throw new IllegalArgumentException("Disk space threshold: "
                    + diskSpaceThreshold + " and lwm threshold: " + diskSpaceLwmThreshold
                    + " are not valid. Should be > 0 and < 1 and diskSpaceThreshold >= diskSpaceLwmThreshold");
        }
    }
}

