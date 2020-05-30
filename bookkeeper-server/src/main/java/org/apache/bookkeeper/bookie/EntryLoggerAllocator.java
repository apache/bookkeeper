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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * An allocator pre-allocates entry log files.
 */
@Slf4j
class EntryLoggerAllocator {

    private long preallocatedLogId;
    Future<BufferedLogChannel> preallocation = null;
    ExecutorService allocatorExecutor;
    private final ServerConfiguration conf;
    private final LedgerDirsManager ledgerDirsManager;
    private final Object createEntryLogLock = new Object();
    private final Object createCompactionLogLock = new Object();
    private final EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private final boolean entryLogPreAllocationEnabled;
    private final ByteBufAllocator byteBufAllocator;
    final ByteBuf logfileHeader = Unpooled.buffer(EntryLogger.LOGFILE_HEADER_SIZE);

    EntryLoggerAllocator(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus, long logId,
            ByteBufAllocator byteBufAllocator) {
        this.conf = conf;
        this.byteBufAllocator = byteBufAllocator;
        this.ledgerDirsManager = ledgerDirsManager;
        this.preallocatedLogId = logId;
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();
        this.allocatorExecutor = Executors.newSingleThreadExecutor();

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        logfileHeader.writeBytes("BKLO".getBytes(UTF_8));
        logfileHeader.writeInt(EntryLogger.HEADER_CURRENT_VERSION);
        logfileHeader.writerIndex(EntryLogger.LOGFILE_HEADER_SIZE);

    }

    synchronized long getPreallocatedLogId() {
        return preallocatedLogId;
    }

    BufferedLogChannel createNewLog(File dirForNextEntryLog) throws IOException {
        synchronized (createEntryLogLock) {
            BufferedLogChannel bc;
            if (!entryLogPreAllocationEnabled){
                // create a new log directly
                bc = allocateNewLog(dirForNextEntryLog);
                return bc;
            } else {
                // allocate directly to response request
                if (null == preallocation){
                    bc = allocateNewLog(dirForNextEntryLog);
                } else {
                    // has a preallocated entry log
                    try {
                        bc = preallocation.get();
                    } catch (ExecutionException ee) {
                        if (ee.getCause() instanceof IOException) {
                            throw (IOException) (ee.getCause());
                        } else {
                            throw new IOException("Error to execute entry log allocation.", ee);
                        }
                    } catch (CancellationException ce) {
                        throw new IOException("Task to allocate a new entry log is cancelled.", ce);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
                    }
                }
                // preallocate a new log in background upon every call
                preallocation = allocatorExecutor.submit(() -> allocateNewLog(dirForNextEntryLog));
                return bc;
            }
        }
    }

    BufferedLogChannel createNewLogForCompaction(File dirForNextEntryLog) throws IOException {
        synchronized (createCompactionLogLock) {
            return allocateNewLog(dirForNextEntryLog, COMPACTING_SUFFIX);
        }
    }

    private synchronized BufferedLogChannel allocateNewLog(File dirForNextEntryLog) throws IOException {
        return allocateNewLog(dirForNextEntryLog, ".log");
    }

    /**
     * Allocate a new log file.
     */
    private synchronized BufferedLogChannel allocateNewLog(File dirForNextEntryLog, String suffix) throws IOException {
        List<File> ledgersDirs = ledgerDirsManager.getAllLedgerDirs();
        String logFileName;
        // It would better not to overwrite existing entry log files
        File testLogFile = null;
        do {
            if (preallocatedLogId >= Integer.MAX_VALUE) {
                preallocatedLogId = 0;
            } else {
                ++preallocatedLogId;
            }
            logFileName = Long.toHexString(preallocatedLogId) + suffix;
            for (File dir : ledgersDirs) {
                testLogFile = new File(dir, logFileName);
                if (testLogFile.exists()) {
                    log.warn("Found existed entry log " + testLogFile
                           + " when trying to create it as a new log.");
                    testLogFile = null;
                    break;
                }
            }
        } while (testLogFile == null);

        File newLogFile = new File(dirForNextEntryLog, logFileName);
        FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();

        BufferedLogChannel logChannel = new BufferedLogChannel(byteBufAllocator, channel, conf.getWriteBufferBytes(),
                conf.getReadBufferBytes(), preallocatedLogId, newLogFile, conf.getFlushIntervalInBytes());
        logfileHeader.readerIndex(0);
        logChannel.write(logfileHeader);

        for (File f : ledgersDirs) {
            setLastLogId(f, preallocatedLogId);
        }

        if (suffix.equals(EntryLogger.LOG_FILE_SUFFIX)) {
            recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
        }

        log.info("Created new entry log file {} for logId {}.", newLogFile, preallocatedLogId);
        return logChannel;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    private void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } catch (IOException e) {
            log.warn("Failed write lastId file");
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                log.error("Could not close lastId file in {}", dir.getPath());
            }
        }
    }

    /**
     * Stop the allocator.
     */
    void stop() {
        // wait until the preallocation finished.
        allocatorExecutor.shutdown();
        try {
            if (!allocatorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Timedout while awaiting for allocatorExecutor's termination, so force shuttingdown");
            }
        } catch (InterruptedException e) {
            log.warn("Got InterruptedException while awaiting termination of allocatorExecutor, so force shuttingdown");
            Thread.currentThread().interrupt();
        }
        allocatorExecutor.shutdownNow();

        log.info("Stopped entry logger preallocator.");
    }

    /**
     * get the preallocation for tests.
     */
    Future<BufferedLogChannel> getPreallocationFuture(){
        return preallocation;
    }
}
