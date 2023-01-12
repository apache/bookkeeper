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
package org.apache.bookkeeper.common.util.nativeio;

import org.apache.commons.lang3.SystemUtils;

class NativeIOJni {

    static native int open(String pathname, int flags, int mode) throws NativeIOException;

    static native int fsync(int fd) throws NativeIOException;

    /**
     * fallocate is a linux-only syscall, so callers must handle the possibility that it does
     * not exist.
     */
    static native int fallocate(int fd, int mode, long offset, long len) throws NativeIOException;

    static native int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException;

    static native long posix_memalign(int alignment, int size) throws NativeIOException;

    static native void free(long pointer) throws NativeIOException;

    static native long lseek(int fd, long offset, int whence) throws NativeIOException;

    static native long pread(int fd, long pointer, long size, long offset) throws NativeIOException;

    static native int close(int fd) throws NativeIOException;

    static {
        try {
            if (SystemUtils.IS_OS_MAC_OSX) {
                NativeUtils.loadLibraryFromJar("/lib/libnative-io.jnilib");
            } else if (SystemUtils.IS_OS_LINUX) {
                NativeUtils.loadLibraryFromJar("/lib/libnative-io.so");
            } else {
                throw new RuntimeException("OS not supported by Native-IO utils");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
