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
import java.util.List;
import java.util.concurrent.ConcurrentMap;

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
class LedgerDirsMonitor extends BookieThread {
    private final static Logger LOG = LoggerFactory.getLogger(LedgerDirsMonitor.class);
    
    private final int interval;
    private final ServerConfiguration conf;
    private final ConcurrentMap<File, Float> diskUsages;
    private final DiskChecker diskChecker;
    private final LedgerDirsManager ldm;

    public LedgerDirsMonitor(final ServerConfiguration conf, 
            final DiskChecker diskChecker,
            final LedgerDirsManager ldm) {
        super("LedgerDirsMonitorThread");
        this.interval = conf.getDiskCheckInterval();
        this.conf = conf;
        this.diskChecker = diskChecker;
        this.diskUsages = ldm.getDiskUsages();
        this.ldm = ldm;
    }

    @Override
    public void run() {
        while (true) {
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
                        LOG.warn("Ledger directory {} is almost full.", dir);
                        diskUsages.put(dir, e.getUsage());
                        for (LedgerDirsListener listener : ldm.getListeners()) {
                            listener.diskAlmostFull(dir);
                        }
                    } catch (DiskOutOfSpaceException e) {
                        LOG.error("Ledger directory {} is out-of-space.", dir);
                        diskUsages.put(dir, e.getUsage());
                        // Notify disk full to all listeners
                        ldm.addToFilledDirs(dir);
                    }
                }
                // Let's get NoWritableLedgerDirException without waiting for the next iteration
                // in case we are out of writable dirs
                // otherwise for the duration of {interval} we end up in the state where 
                // bookie cannot get writable dir but considered to be writable 
                ldm.getWritableLedgerDirs();
            } catch (NoWritableLedgerDirException e) {
                for (LedgerDirsListener listener : ldm.getListeners()) {
                    listener.allDisksFull();
                }
            }

            List<File> fullfilledDirs = new ArrayList<File>(ldm.getFullFilledLedgerDirs());
            boolean hasWritableLedgerDirs = ldm.hasWritableLedgerDirs();
            float totalDiskUsage = 0;

            // When bookie is in READONLY mode .i.e there are no writableLedgerDirs:
            // - Check if the total disk usage is below DiskLowWaterMarkUsageThreshold.
            // - If So, walk through the entire list of fullfilledDirs and add them back to writableLedgerDirs list if
            // their usage is < conf.getDiskUsageThreshold.
            try {
                if (hasWritableLedgerDirs
                        || (totalDiskUsage = diskChecker.getTotalDiskUsage(ldm.getAllLedgerDirs())) < conf
                                .getDiskLowWaterMarkUsageThreshold()) {
                    // Check all full-filled disk space usage
                    for (File dir : fullfilledDirs) {
                        try {
                            diskUsages.put(dir, diskChecker.checkDir(dir));
                            ldm.addToWritableDirs(dir, true);
                        } catch (DiskErrorException e) {
                            // Notify disk failure to all the listeners
                            for (LedgerDirsListener listener : ldm.getListeners()) {
                                listener.diskFailed(dir);
                            }
                        } catch (DiskWarnThresholdException e) {
                            diskUsages.put(dir, e.getUsage());
                            // the full-filled dir become writable but still
                            // above
                            // warn threshold
                            ldm.addToWritableDirs(dir, false);
                        } catch (DiskOutOfSpaceException e) {
                            // the full-filled dir is still full-filled
                            diskUsages.put(dir, e.getUsage());
                        }
                    }
                } else {
                    LOG.debug(
                            "Current TotalDiskUsage: {} is greater than LWMThreshold: {}. So not adding any filledDir to WritableDirsList",
                            totalDiskUsage, conf.getDiskLowWaterMarkUsageThreshold());
                }
            } catch (IOException ioe) {
                LOG.error("Got IOException while monitoring Dirs", ioe);
                for (LedgerDirsListener listener : ldm.getListeners()) {
                    listener.fatalError();
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
        checkDirs(ldm.getWritableLedgerDirs());
    }

    // start the daemon for disk monitoring
    @Override
    public void start() {
        this.setDaemon(true);
        super.start();
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
        LOG.info("Shutting down LedgerDirsMonitor");
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    public void checkDirs(List<File> writableDirs)
            throws DiskErrorException, NoWritableLedgerDirException {
        for (File dir : writableDirs) {
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
}

