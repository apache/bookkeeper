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

import java.lang.reflect.Field;
import java.io.FileDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NativeIO {
    private static final Logger LOG = LoggerFactory.getLogger(NativeIO.class);

    private static final int POSIX_FADV_DONTNEED = 4; /* fadvise.h */

    private static final int FALLOC_FL_KEEP_SIZE = 1;

    private static boolean initialized = false;
    private static boolean fadvisePossible = true;
    private static boolean sysFallocatePossible = true;
    private static boolean posixFallocatePossible = true;

    static {
        try {
            LOG.info("Loading bookkeeper native library.");
            System.loadLibrary("bookkeeper");
            initialized = true;
            LOG.info("Loaded bookkeeper native library. Enabled Native IO.");
        } catch (Throwable t) {
            LOG.info("Unable to load bookkeeper native library. Native methods will be disabled : ", t);
        }
    }

    // fadvice
    public static native int posix_fadvise(int fd, long offset, long len, int flag);
    // posix_fallocate
    public static native int posix_fallocate(int fd, long offset, long len);
    // fallocate
    public static native int fallocate(int fd, int mode, long offset, long len);

    private NativeIO() {}

    private static Field getFieldByReflection(Class cls, String fieldName) {
        Field field = null;

        try {
            field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (Exception e) {
            // We don't really expect this so throw an assertion to
            // catch this during development
            assert false;
            LOG.warn("Unable to read {} field from {}", fieldName, cls.getName());
        }

        return field;
    }

    /**
     * Get system file descriptor (int) from FileDescriptor object.
     * @param descriptor - FileDescriptor object to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getSysFileDescriptor(FileDescriptor descriptor) {
        Field field = getFieldByReflection(descriptor.getClass(), "fd");
        try {
            return field.getInt(descriptor);
        } catch (Exception e) {
            LOG.warn("Unable to read fd field from java.io.FileDescriptor");
        }

        return -1;
    }

    public static boolean fallocateIfPossible(int fd, long offset, long nbytes) {
        if (!initialized || fd < 0) {
            return false;
        }
        boolean allocated = false;
        if (sysFallocatePossible) {
            allocated = sysFallocateIfPossible(fd, offset, nbytes);
        }
        if (!allocated && posixFallocatePossible) {
            allocated = posixFallocateIfPossible(fd, offset, nbytes);
        }
        return allocated;
    }

    private static boolean sysFallocateIfPossible(int fd, long offset, long nbytes) {
        try {
            int rc = fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, nbytes);
            if (rc != 0) {
                LOG.error("Failed on sys fallocate file descriptor {}, offset {}, bytes {}, rc {} : {}",
                        new Object[] { fd, offset, nbytes, rc, Errno.strerror() });
                return false;
            }
        } catch (UnsupportedOperationException uoe) {
            LOG.warn("sys fallocate isn't supported : ", uoe);
            sysFallocatePossible = false;
        }  catch (UnsatisfiedLinkError nle) {
            LOG.warn("Unsatisfied Link error: sys fallocate failed on file descriptor {}, offset {}, bytes {} : ",
                    new Object[] { fd, offset, nbytes, nle });
            sysFallocatePossible = false;
        } catch (Exception e) {
            LOG.error("Unknown exception: sys fallocate failed on file descriptor {}, offset {}, bytes {} : ",
                    new Object[] { fd, offset, nbytes, e });
            return false;
        }
        return sysFallocatePossible;
    }

    private static boolean posixFallocateIfPossible(int fd, long offset, long nbytes) {
        try {
            int rc = posix_fallocate(fd, offset, nbytes);
            if (rc != 0) {
                LOG.error("Failed on posix_fallocate file descriptor {}, offset {}, bytes {}, rc {} : {}",
                        new Object[] { fd, offset, nbytes, rc, Errno.strerror() });
                return false;
            }
        } catch (UnsupportedOperationException uoe) {
            LOG.warn("posix_fallocate isn't supported : ", uoe);
            posixFallocatePossible = false;
        }  catch (UnsatisfiedLinkError nle) {
            LOG.warn("Unsatisfied Link error: posix_fallocate failed on file descriptor {}, offset {}, bytes {} : ",
                    new Object[] { fd, offset, nbytes, nle });
            posixFallocatePossible = false;
        } catch (Exception e) {
            LOG.error("Unknown exception: posix_fallocate failed on file descriptor {}, offset {}, bytes {} : ",
                    new Object[] { fd, offset, nbytes, e });
            return false;
        }
        return posixFallocatePossible;
    }

    /**
     * Remove pages from the file system page cache when they wont
     * be accessed again
     *
     * @param fd     The file descriptor of the source file.
     * @param offset The offset within the file.
     * @param len    The length to be flushed.
     */
    public static void bestEffortRemoveFromPageCache(int fd, long offset, long len) {
        posixFadviseIfPossible(fd, offset, len, POSIX_FADV_DONTNEED);
    }

    public static boolean posixFadviseIfPossible(int fd, long offset, long len, int flags) {
        if (!initialized || !fadvisePossible || fd < 0) {
            return false;
        }
        try {
            int rc = posix_fadvise(fd, offset, len, flags);
            if (rc != 0) {
                LOG.error("Failed on posix_fadvise file descriptor {}, offset {}, bytes {}, flags {}, rc {} : {}",
                        new Object[] { fd, offset, len, flags, rc, Errno.strerror() });
                return false;
            }
        } catch (UnsupportedOperationException uoe) {
            LOG.warn("posix_fadvise is not supported : ", uoe);
            fadvisePossible = false;
        } catch (UnsatisfiedLinkError ule) {
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
            LOG.warn("Unsatisfied Link error: posix_fadvise failed on file descriptor {}, offset {} : ",
                    new Object[] { fd, offset, ule });
            fadvisePossible = false;
        } catch (Exception e) {
            // This is best effort anyway so lets just log that there was an
            // exception and forget
            LOG.warn("Unknown exception: posix_fadvise failed on file descriptor {}, offset {} : ",
                    new Object[] { fd, offset, e });
            return false;
        }
        return fadvisePossible;
    }

}
