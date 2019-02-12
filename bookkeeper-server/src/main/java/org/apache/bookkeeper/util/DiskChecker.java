/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.util;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides utility functions for checking disk problems.
 */
public class DiskChecker {

    private static final Logger LOG = LoggerFactory.getLogger(DiskChecker.class);

    private float diskUsageThreshold;
    private float diskUsageWarnThreshold;

    /**
     * A general marker for disk-related exceptions.
     */
    public abstract static class DiskException extends IOException {
        public DiskException(String msg) {
            super(msg);
        }
    }

    /**
     * A disk error exception.
     */
    public static class DiskErrorException extends DiskException {
        private static final long serialVersionUID = 9091606022449761729L;

        public DiskErrorException(String msg) {
            super(msg);
        }
    }

    /**
     * An out-of-space disk exception.
     */
    public static class DiskOutOfSpaceException extends DiskException {
        private static final long serialVersionUID = 160898797915906860L;

        private final float usage;

        public DiskOutOfSpaceException(String msg, float usage) {
            super(msg);
            this.usage = usage;
        }

        public float getUsage() {
            return usage;
        }
    }

    /**
     * A disk warn threshold exception.
     */
    public static class DiskWarnThresholdException extends DiskException {
        private static final long serialVersionUID = -1629284987500841657L;

        private final float usage;

        public DiskWarnThresholdException(String msg, float usage) {
            super(msg);
            this.usage = usage;
        }

        public float getUsage() {
            return usage;
        }
    }

    public DiskChecker(float threshold, float warnThreshold) {
        validateThreshold(threshold, warnThreshold);
        this.diskUsageThreshold = threshold;
        this.diskUsageWarnThreshold = warnThreshold;
    }

    /**
     * The semantics of mkdirsWithExistsCheck method is different from the
     * mkdirs method provided in the Sun's java.io.File class in the following
     * way: While creating the non-existent parent directories, this method
     * checks for the existence of those directories if the mkdir fails at any
     * point (since that directory might have just been created by some other
     * process). If both mkdir() and the exists() check fails for any seemingly
     * non-existent directory, then we signal an error; Sun's mkdir would signal
     * an error (return false) if a directory it is attempting to create already
     * exists or the mkdir fails.
     *
     * @param dir
     * @return true on success, false on failure
     */
    private static boolean mkdirsWithExistsCheck(File dir) {
        if (dir.mkdir() || dir.exists()) {
            return true;
        }
        File canonDir = null;
        try {
            canonDir = dir.getCanonicalFile();
        } catch (IOException e) {
            return false;
        }
        String parent = canonDir.getParent();
        return (parent != null)
                && (mkdirsWithExistsCheck(new File(parent)) && (canonDir
                        .mkdir() || canonDir.exists()));
    }

    /**
     * Checks the disk space available.
     *
     * @param dir
     *            Directory to check for the disk space
     * @throws DiskOutOfSpaceException
     *             Throws {@link DiskOutOfSpaceException} if available space is
     *             less than threshhold.
     */
    @VisibleForTesting
    float checkDiskFull(File dir) throws DiskOutOfSpaceException, DiskWarnThresholdException {
        if (null == dir) {
            return 0f;
        }
        if (dir.exists()) {
            long usableSpace = dir.getUsableSpace();
            long totalSpace = dir.getTotalSpace();
            float free = (float) usableSpace / (float) totalSpace;
            float used = 1f - free;
            if (used > diskUsageThreshold) {
                LOG.error("Space left on device {} : {}, Used space fraction: {} > threshold {}.",
                        dir, usableSpace, used, diskUsageThreshold);
                throw new DiskOutOfSpaceException("Space left on device "
                        + usableSpace + " Used space fraction:" + used + " > threshold " + diskUsageThreshold, used);
            }
            // Warn should be triggered only if disk usage threshold doesn't trigger first.
            if (used > diskUsageWarnThreshold) {
                LOG.warn("Space left on device {} : {}, Used space fraction: {} > WarnThreshold {}.",
                        dir, usableSpace, used, diskUsageWarnThreshold);
                throw new DiskWarnThresholdException("Space left on device:"
                        + usableSpace + " Used space fraction:" + used + " > WarnThreshold:" + diskUsageWarnThreshold,
                        used);
            }
            return used;
        } else {
            return checkDiskFull(dir.getParentFile());
        }
    }


    /**
     * Calculate the total amount of free space available
     * in all of the ledger directories put together.
     *
     * @return totalDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalFreeSpace(List<File> dirs) throws IOException {
        long totalFreeSpace = 0;
        Set<FileStore> dirsFileStore = new HashSet<FileStore>();
        for (File dir : dirs) {
            FileStore fileStore = Files.getFileStore(dir.toPath());
            if (dirsFileStore.add(fileStore)) {
                totalFreeSpace += fileStore.getUsableSpace();
            }
        }
        return totalFreeSpace;
    }

    /**
     * Calculate the total amount of free space available
     * in all of the ledger directories put together.
     *
     * @return freeDiskSpace in bytes
     * @throws IOException
     */
    public long getTotalDiskSpace(List<File> dirs) throws IOException {
        long totalDiskSpace = 0;
        Set<FileStore> dirsFileStore = new HashSet<FileStore>();
        for (File dir : dirs) {
            FileStore fileStore = Files.getFileStore(dir.toPath());
            if (dirsFileStore.add(fileStore)) {
                totalDiskSpace += fileStore.getTotalSpace();
            }
        }
        return totalDiskSpace;
    }

    /**
     * calculates and returns the disk usage factor in the provided list of dirs.
     *
     * @param dirs
     *            list of directories
     * @return disk usage factor in the provided list of dirs
     * @throws IOException
     */
    public float getTotalDiskUsage(List<File> dirs) throws IOException {
        if (dirs == null || dirs.isEmpty()) {
            throw new IllegalArgumentException(
                    "list argument of getTotalDiskUsage is not supposed to be null or empty");
        }
        float free = (float) getTotalFreeSpace(dirs) / (float) getTotalDiskSpace(dirs);
        float used = 1f - free;
        return used;
    }

    /**
     * Create the directory if it doesn't exist.
     *
     * @param dir
     *            Directory to check for the disk error/full.
     * @throws DiskErrorException
     *             If disk having errors
     * @throws DiskWarnThresholdException
     *             If disk has less than configured amount of free space.
     * @throws DiskOutOfSpaceException
     *             If disk is full or having less space than threshhold
     */
    public float checkDir(File dir) throws DiskErrorException,
            DiskOutOfSpaceException, DiskWarnThresholdException {
        float usage = checkDiskFull(dir);
        if (!mkdirsWithExistsCheck(dir)) {
            throw new DiskErrorException("can not create directory: "
                    + dir.toString());
        }

        if (!dir.isDirectory()) {
            throw new DiskErrorException("not a directory: " + dir.toString());
        }

        if (!dir.canRead()) {
            throw new DiskErrorException("directory is not readable: "
                    + dir.toString());
        }

        if (!dir.canWrite()) {
            throw new DiskErrorException("directory is not writable: "
                    + dir.toString());
        }
        return usage;
    }

    /**
     * Set the disk space threshold.
     *
     * @param diskSpaceThreshold
     */
    void setDiskSpaceThreshold(float diskSpaceThreshold, float diskUsageWarnThreshold) {
        validateThreshold(diskSpaceThreshold, diskUsageWarnThreshold);
        this.diskUsageThreshold = diskSpaceThreshold;
        this.diskUsageWarnThreshold = diskUsageWarnThreshold;
    }

    private void validateThreshold(float diskSpaceThreshold, float diskSpaceWarnThreshold) {
        if (diskSpaceThreshold <= 0 || diskSpaceThreshold >= 1 || diskSpaceWarnThreshold - diskSpaceThreshold > 1e-6) {
            throw new IllegalArgumentException("Disk space threashold: "
                    + diskSpaceThreshold + " and warn threshold: " + diskSpaceWarnThreshold
                    + " are not valid. Should be > 0 and < 1 and diskSpaceThreshold >= diskSpaceWarnThreshold");
        }
    }

    public float getDiskUsageThreshold() {
        return diskUsageThreshold;
    }

    public float getDiskUsageWarnThreshold() {
        return diskUsageWarnThreshold;
    }
}
