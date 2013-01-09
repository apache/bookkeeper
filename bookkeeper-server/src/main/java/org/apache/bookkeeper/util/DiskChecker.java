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

import java.io.File;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class that provides utility functions for checking disk problems
 */
public class DiskChecker {
    private float diskUsageThreshold;

    public static class DiskErrorException extends IOException {
        private static final long serialVersionUID = 9091606022449761729L;

        public DiskErrorException(String msg) {
            super(msg);
        }
    }

    public static class DiskOutOfSpaceException extends IOException {
        private static final long serialVersionUID = 160898797915906860L;

        public DiskOutOfSpaceException(String msg) {
            super(msg);
        }
    }

    public DiskChecker(float threshold) {
        validateThreshold(threshold);
        this.diskUsageThreshold = threshold;
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
    void checkDiskFull(File dir) throws DiskOutOfSpaceException {
        if (null == dir) {
            return;
        }
        if (dir.exists()) {
            long usableSpace = dir.getUsableSpace();
            long totalSpace = dir.getTotalSpace();
            float free = (float) usableSpace / (float) totalSpace;
            float used = 1f - free;
            if (used > diskUsageThreshold) {
                throw new DiskOutOfSpaceException("Space left on device "
                        + usableSpace + " < threshhold " + diskUsageThreshold);
            }
        } else {
            checkDiskFull(dir.getParentFile());
        }
    }

    /**
     * Create the directory if it doesn't exist and
     * 
     * @param dir
     *            Directory to check for the disk error/full.
     * @throws DiskErrorException
     *             If disk having errors
     * @throws DiskOutOfSpaceException
     *             If disk is full or having less space than threshhold
     */
    public void checkDir(File dir) throws DiskErrorException,
            DiskOutOfSpaceException {
        checkDiskFull(dir);
        if (!mkdirsWithExistsCheck(dir))
            throw new DiskErrorException("can not create directory: "
                    + dir.toString());

        if (!dir.isDirectory())
            throw new DiskErrorException("not a directory: " + dir.toString());

        if (!dir.canRead())
            throw new DiskErrorException("directory is not readable: "
                    + dir.toString());

        if (!dir.canWrite())
            throw new DiskErrorException("directory is not writable: "
                    + dir.toString());
    }

    /**
     * Returns the disk space threshold.
     * 
     * @return
     */
    @VisibleForTesting
    float getDiskSpaceThreshold() {
        return diskUsageThreshold;
    }

    /**
     * Set the disk space threshold
     * 
     * @param diskSpaceThreshold
     */
    @VisibleForTesting
    void setDiskSpaceThreshold(float diskSpaceThreshold) {
        validateThreshold(diskSpaceThreshold);
        this.diskUsageThreshold = diskSpaceThreshold;
    }

    private void validateThreshold(float diskSpaceThreshold) {
        if (diskSpaceThreshold <= 0 || diskSpaceThreshold >= 1) {
            throw new IllegalArgumentException("Disk space threashold "
                    + diskSpaceThreshold
                    + " is not valid. Should be > 0 and < 1 ");
        }
    }
}
