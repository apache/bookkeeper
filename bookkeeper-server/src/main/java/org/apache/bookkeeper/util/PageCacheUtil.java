/*
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

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;

/**
 * Native I/O operations.
 */
@UtilityClass
@Slf4j
public final class PageCacheUtil {

    private static final int POSIX_FADV_DONTNEED = 4; /* fadvise.h */

    private static boolean fadvisePossible = true;

    private static final NativeIO NATIVE_IO;

    static {
        NativeIO nativeIO = null;
        try {
            nativeIO = new NativeIOImpl();
        } catch (Exception e) {
            log.warn("Unable to initialize NativeIO for posix_fdavise: {}", e.getMessage());
            fadvisePossible = false;
        }

        NATIVE_IO = nativeIO;
    }

    private static Field getFieldByReflection(Class cls, String fieldName) {
        Field field = null;

        try {
            field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (Exception e) {
            // We don't really expect this so throw an assertion to
            // catch this during development
            log.warn("Unable to read {} field from {}", fieldName, cls.getName());
            assert false;
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
            log.warn("Unable to read fd field from java.io.FileDescriptor");
        }

        return -1;
    }

    /**
     * Remove pages from the file system page cache when they won't
     * be accessed again.
     *
     * @param fd     The file descriptor of the source file.
     * @param offset The offset within the file.
     * @param len    The length to be flushed.
     */
    public static void bestEffortRemoveFromPageCache(int fd, long offset, long len) {
        if (!fadvisePossible || fd < 0) {
            return;
        }
        try {
            NATIVE_IO.posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
        } catch (Throwable e) {
            log.warn("Failed to perform posix_fadvise: {}", e.getMessage());
            fadvisePossible = false;
        }
    }
}
