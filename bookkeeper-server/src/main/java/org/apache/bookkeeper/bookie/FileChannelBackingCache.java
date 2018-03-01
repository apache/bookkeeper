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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileChannelBackingCache used to cache RefCntFileChannels for read.
 * In order to avoid get released file, adopt design of FileInfoBackingCache.
 * @see FileInfoBackingCache
 */
class FileChannelBackingCache {
    private static final Logger LOG = LoggerFactory.getLogger(FileChannelBackingCache.class);
    static final int DEAD_REF = -0xdead;
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    final FileLoader fileLoader;

    FileChannelBackingCache(FileLoader fileLoader) {
        this.fileLoader = fileLoader;
    }

    final ConcurrentHashMap<Long, CachedFileChannel> fileChannels = new ConcurrentHashMap<>();

    CachedFileChannel loadFileChannel(long logId) throws IOException {
        lock.readLock().lock();
        try {
            CachedFileChannel cachedFileChannel = fileChannels.get(logId);
            if (cachedFileChannel != null) {
                boolean retained = cachedFileChannel.tryRetain();
                checkArgument(retained);
                return cachedFileChannel;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            File file = fileLoader.load(logId);
            // get channel is used to open an existing entry log file
            // it would be better to open using read mode
            FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
            CachedFileChannel cachedFileChannel = new CachedFileChannel(logId, newFc);
            fileChannels.put(logId, cachedFileChannel);
            boolean retained = cachedFileChannel.tryRetain();
            checkArgument(retained);
            return cachedFileChannel;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * close FileChannel and remove from cache when possible.
     * @param logId
     * @param cachedFileChannel
     */
    private void releaseFileChannel(long logId, CachedFileChannel cachedFileChannel) {
        lock.writeLock().lock();
        try {
            if (cachedFileChannel.markDead()) {
                try {
                    cachedFileChannel.fileChannel.close();
                } catch (IOException e) {
                    LOG.warn("Exception occurred in ReferenceCountedFileChannel"
                            + " while closing channel for log file: {}", cachedFileChannel);
                } finally {
                    IOUtils.close(LOG, cachedFileChannel.fileChannel);
                }
                // to guarantee the removed cachedFileChannel is what we want to remove.
                fileChannels.remove(logId, cachedFileChannel);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove all entries for this log file in each thread's cache.
     * @param logId
     */
    public void removeFromChannelsAndClose(long logId) {
        //remove the fileChannel from FileChannelBackingCache and close it
        CachedFileChannel fileChannel = fileChannels.remove(logId);
        fileChannel.release();
        try {
            fileChannel.fileChannel.close();
        } catch (IOException e) {
            LOG.warn("Exception occurred in CachedFileChannel"
                    + " while closing channel for log file: {}", logId);
        } finally {
            IOUtils.close(LOG, fileChannel.fileChannel);
        }
    }

    void closeAllFileChannels() throws IOException {
        for (Map.Entry<Long, CachedFileChannel> entry : fileChannels.entrySet()) {
            entry.getValue().fileChannel.close();
        }
    }

    @VisibleForTesting
    CachedFileChannel get(Long logId) {
        lock.readLock().lock();
        try {
            return fileChannels.get(logId);
        } finally {
            lock.readLock().unlock();
        }
    }

    class CachedFileChannel {
        private final long entryLogId;
        final AtomicInteger refCount;
        final FileChannel fileChannel;

        CachedFileChannel(long entryLogId, FileChannel fileChannel) throws IOException{
            this.fileChannel = fileChannel;
            this.entryLogId = entryLogId;
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
         * Attempt to retain the file channel.
         * When a client obtains a file channel from a container object,
         * but that container object may release the file channel before
         * the client has a chance to call retain. In this case, the
         * file channel could be released and the destroyed before we ever
         * get a chance to use it.
         *
         * <p>tryRetain avoids this problem, by doing a compare-and-swap on
         * the reference count. If the refCount is negative, it means that
         * the file channel is being cleaned up, and this file channel object should
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

        @VisibleForTesting
        int getRefCount() {
            return refCount.get();
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                releaseFileChannel(entryLogId, this);
            }
        }

        @Override
        public String toString() {
            return "CachedFileChannel(logId=" + entryLogId
                    + ",refCount=" + refCount.get()
                    + ",id=" + System.identityHashCode(this) + ")";
        }
    }
    interface FileLoader {
        File load(long logId) throws IOException;
    }
}

