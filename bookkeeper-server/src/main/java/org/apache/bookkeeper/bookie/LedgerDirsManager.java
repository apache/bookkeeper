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

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages ledger directories used by the bookie.
 */
public class LedgerDirsManager {
    private static Logger LOG = LoggerFactory
            .getLogger(LedgerDirsManager.class);

    private volatile List<File> filledDirs;
    private final List<File> ledgerDirectories;
    private volatile List<File> writableLedgerDirectories;
    private DiskChecker diskChecker;
    private List<LedgerDirsListener> listeners;
    private LedgerDirsMonitor monitor;
    private final Random rand = new Random();

    public LedgerDirsManager(ServerConfiguration conf) {
        this.ledgerDirectories = Arrays.asList(Bookie
                .getCurrentDirectories(conf.getLedgerDirs()));
        this.writableLedgerDirectories = new ArrayList<File>(ledgerDirectories);
        this.filledDirs = new ArrayList<File>();
        listeners = new ArrayList<LedgerDirsManager.LedgerDirsListener>();
        diskChecker = new DiskChecker(conf.getDiskUsageThreshold());
        monitor = new LedgerDirsMonitor(conf.getDiskCheckInterval());
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

    // start the daemon for disk monitoring
    public void start() {
        monitor.setDaemon(true);
        monitor.start();
    }

    // shutdown disk monitoring daemon
    public void shutdown() {
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
    private class LedgerDirsMonitor extends Thread {
        private final int interval;

        public LedgerDirsMonitor(int interval) {
            super("LedgerDirsMonitorThread");
            this.interval = interval;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    List<File> writableDirs;
                    try {
                        writableDirs = getWritableLedgerDirs();
                    } catch (NoWritableLedgerDirException e) {
                        for (LedgerDirsListener listener : listeners) {
                            listener.allDisksFull();
                        }
                        break;
                    }
                    // Check all writable dirs disk space usage.
                    for (File dir : writableDirs) {
                        try {
                            diskChecker.checkDir(dir);
                        } catch (DiskErrorException e) {
                            // Notify disk failure to all listeners
                            for (LedgerDirsListener listener : listeners) {
                                listener.diskFailed(dir);
                            }
                        } catch (DiskOutOfSpaceException e) {
                            // Notify disk full to all listeners
                            addToFilledDirs(dir);
                        }
                    }
                    try {
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        LOG.info("LedgerDirsMonitor thread is interrupted");
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error Occured while checking disks", e);
                // Notify disk failure to all listeners
                for (LedgerDirsListener listener : listeners) {
                    listener.fatalError();
                }
            }
            LOG.info("LedgerDirsMonitorThread exited!");
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
         * This will be notified on disk detected as full
         * 
         * @param disk
         *            Filled disk
         */
        void diskFull(File disk);

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
