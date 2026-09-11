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

import java.util.List;
import org.apache.bookkeeper.common.util.nativelib.NativeUtils;
import org.apache.commons.lang3.SystemUtils;

class NativeIOJni {

    static native int open(String pathname, int flags, int mode) throws NativeIOException;

    static native int fsync(int fd) throws NativeIOException;

    /**
     * fallocate is a linux-only syscall, so callers must handle the possibility that it does
     * not exist.
     */
    static native int fallocate(int fd, int mode, long offset, long len) throws NativeIOException;

    static native int posix_fadvise(int fd, long offset, long len, int flag) throws NativeIOException;

    static native int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException;

    static native long posix_memalign(int alignment, int size) throws NativeIOException;

    static native void free(long pointer) throws NativeIOException;

    static native long lseek(int fd, long offset, int whence) throws NativeIOException;

    static native long pread(int fd, long pointer, long size, long offset) throws NativeIOException;

    static native int close(int fd) throws NativeIOException;

    static {
        String explicitPath = NativeIOLibraryPath.configuredLibraryPath();
        if (explicitPath != null) {
            System.load(explicitPath);
        } else {
            List<String> candidates = NativeIOLibraryPath.currentPlatformLibraryCandidates();
            if (candidates.isEmpty()) {
                throw new IllegalStateException("No native-io JNI library candidates found for platform "
                        + SystemUtils.OS_NAME + "/" + SystemUtils.OS_ARCH);
            }

            boolean loaded = false;
            Throwable lastFailure = null;
            for (String candidate : candidates) {
                try {
                    NativeUtils.loadLibraryFromJar(candidate);
                    loaded = true;
                    break;
                } catch (Exception | UnsatisfiedLinkError e) {
                    lastFailure = e;
                }
            }

            if (!loaded) {
                throw new IllegalStateException("Failed to load any native-io JNI library candidate for platform "
                        + SystemUtils.OS_NAME + "/" + SystemUtils.OS_ARCH, lastFailure);
            }
        }
    }
}
