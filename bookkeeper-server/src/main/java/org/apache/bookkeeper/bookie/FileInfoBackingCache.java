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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class FileInfoBackingCache {
    static final int DEAD_REF = -0xdead;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    final ConcurrentHashMap<Long, CachedFileInfo> fileInfos = new ConcurrentHashMap<>();
    final FileLoader fileLoader;

    FileInfoBackingCache(FileLoader fileLoader) {
        this.fileLoader = fileLoader;
    }

    CachedFileInfo loadFileInfo(long ledgerId, byte[] masterKey) throws IOException {
        lock.readLock().lock();
        try {
            CachedFileInfo fi = fileInfos.get(ledgerId);
            if (fi != null) {
                // tryRetain only fails if #markDead() has been called
                // on fi. This is only called from within the write lock,
                // and if it is called (and succeeds) the fi will have been
                // removed from fileInfos at the same time, so we should not
                // have been able to get a reference to it here.
                // The caller of loadFileInfo owns the refence, and is
                // responsible for calling the corresponding #release().
                assert(fi.tryRetain());
                return fi;
            }
        } finally {
            lock.readLock().unlock();
        }

        // else FileInfo not found, create it under write lock
        lock.writeLock().lock();
        try {
            File backingFile = fileLoader.load(ledgerId, masterKey != null);
            CachedFileInfo fi = new CachedFileInfo(ledgerId, backingFile, masterKey);
            fileInfos.put(ledgerId, fi);

            // see comment above for why we assert
            assert(fi.tryRetain());
            return fi;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void releaseFileInfo(long ledgerId, CachedFileInfo fileInfo) {
        lock.writeLock().lock();
        try {
            if (fileInfo.markDead()) {
                fileInfo.close(true);
                fileInfos.remove(ledgerId, fileInfo);
            }
        } catch (IOException ioe) {
            log.error("Error evicting file info({}) for ledger {} from backing cache",
                      fileInfo, ledgerId, ioe);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void closeAllWithoutFlushing() throws IOException {
        for (Map.Entry<Long, CachedFileInfo> entry : fileInfos.entrySet()) {
            entry.getValue().close(false);
        }
    }

    class CachedFileInfo extends FileInfo {
        final long ledgerId;
        final AtomicInteger refCount;

        CachedFileInfo(long ledgerId, File lf, byte[] masterKey) throws IOException {
            super(lf, masterKey);
            this.ledgerId = ledgerId;
            this.refCount = new AtomicInteger(0);
        }

        /**
         * Mark this fileinfo as dead. We can only mark a fileinfo as
         * dead if noone currently holds a reference to it.
         *
         * @return true if we marked as dead, false otherwise
         */
        private boolean markDead() {
            return refCount.compareAndSet(0, DEAD_REF);
        }

        /**
         * Attempt to retain the file info.
         * When a client obtains a fileinfo from a container object,
         * but that container object may release the fileinfo before
         * the client has a chance to call retain. In this case, the
         * file info could be released and the destroyed before we ever
         * get a chance to use it.
         *
         * <p>tryRetain avoids this problem, by doing a compare-and-swap on
         * the reference count. If the refCount is negative, it means that
         * the fileinfo is being cleaned up, and this fileinfo object should
         * not be used. This works in tandem with #markDead, which will only
         * set the refCount to negative if noone currently has it retained
         * (i.e. the refCount is 0).
         *
         * @return true if we managed to increment the refcount, false otherwise
         */
        boolean tryRetain() {
            while (true) {
                int count = refCount.get();
                if (count < 0) {
                    return false;
                } else if (refCount.compareAndSet(count, count + 1)) {
                    return true;
                }
            }
        }

        int getRefCount() {
            return refCount.get();
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                releaseFileInfo(ledgerId, this);
            }
        }

        @Override
        public String toString() {
            return "CachedFileInfo(ledger=" + ledgerId
                + ",refCount=" + refCount.get()
                + ",closed=" + isClosed()
                + ",id=" + System.identityHashCode(this) + ")";
        }
    }

    interface FileLoader {
        File load(long ledgerId, boolean createIfMissing) throws IOException;
    }
}
