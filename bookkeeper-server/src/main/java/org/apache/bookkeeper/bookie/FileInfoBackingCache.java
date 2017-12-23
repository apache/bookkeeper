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
                fi.retain();  // caller of loadFileInfo owns this reference
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

            fi.retain(); // caller of loadFileInfo owns this reference
            return fi;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void releaseFileInfo(long ledgerId, CachedFileInfo fileInfo) {
        lock.writeLock().lock();
        try {
            /* RefCount may have been incremented between the call
             * to decrementAndGet in CachedFileInfo#release and getting
             * to this point in the code, so check it again.
             * If the refCount for a fileInfo is 0, it means that the only
             * object referencing it is the fileInfos map. Acquiring the
             * fileInfo from the map is done under the lock, so if the
             * refcount is 0 here, we'll be able to remove it from the
             * map before anyone has a chance to increment it.
             */
            if (fileInfo.getRefCount() == 0) {
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

        void retain() {
            refCount.incrementAndGet();
        }

        int getRefCount() {
            return refCount.get();
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                releaseFileInfo(ledgerId, this);
            }
        }
    }

    interface FileLoader {
        File load(long ledgerId, boolean createIfMissing) throws IOException;
    }
}
