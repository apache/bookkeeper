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

import static org.apache.bookkeeper.bookie.DefaultEntryLogger.UNASSIGNED_LEDGERID;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;

@CustomLog
abstract class EntryLogManagerBase implements EntryLogManager {
    volatile List<BufferedLogChannel> rotatedLogChannels;
    final EntryLoggerAllocator entryLoggerAllocator;
    final LedgerDirsManager ledgerDirsManager;
    private final List<DefaultEntryLogger.EntryLogListener> listeners;
    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;

    EntryLogManagerBase(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<DefaultEntryLogger.EntryLogListener> listeners) {
        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLoggerAllocator = entryLoggerAllocator;
        this.listeners = listeners;
        this.logSizeLimit = conf.getEntryLogSizeLimit();
    }

    private final FastThreadLocal<ByteBuf> sizeBufferForAdd = new FastThreadLocal<ByteBuf>() {
        @Override
        protected ByteBuf initialValue() throws Exception {
            return Unpooled.buffer(4);
        }
    };

    /*
     * This method should be guarded by a lock, so callers of this method
     * should be in the right scope of the lock.
     */
    @Override
    public long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        int entrySize = entry.readableBytes() + 4; // Adding 4 bytes to prepend the size
        BufferedLogChannel logChannel = getCurrentLogForLedgerForAddEntry(ledger, entrySize, rollLog);
        ByteBuf sizeBuffer = sizeBufferForAdd.get();
        sizeBuffer.clear();
        sizeBuffer.writeInt(entry.readableBytes());
        try {
            logChannel.write(sizeBuffer);

            long pos = logChannel.position();
            logChannel.write(entry);
            logChannel.registerWrittenEntry(ledger, entrySize);

            return (logChannel.getLogId() << 32L) | pos;
        } catch (EntryLogWriteException e) {
            throw e;
        } catch (IOException e) {
            throw new EntryLogWriteException(
                    "Failed to write entry to entry log " + logChannel.getLogId() + " for ledger " + ledger, e);
        }
    }

    boolean reachEntryLogLimit(BufferedLogChannel logChannel, long size) {
        if (logChannel == null) {
            return false;
        }
        return logChannel.position() + size > logSizeLimit;
    }

    boolean readEntryLogHardLimit(BufferedLogChannel logChannel, long size) {
        if (logChannel == null) {
            return false;
        }
        return logChannel.position() + size > Integer.MAX_VALUE;
    }

    abstract BufferedLogChannel getCurrentLogForLedger(long ledgerId) throws IOException;

    abstract BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize, boolean rollLog)
            throws IOException;

    abstract void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) throws IOException;

    /*
     * flush current logs.
     */
    abstract void flushCurrentLogs() throws IOException;

    /*
     * flush rotated logs.
     */
    abstract void flushRotatedLogs() throws IOException;

    List<BufferedLogChannel> getRotatedLogChannels() {
        return rotatedLogChannels;
    }

    @Override
    public void flush() throws IOException {
        flushCurrentLogs();
        flushRotatedLogs();
    }

    void flushLogChannel(BufferedLogChannel logChannel, boolean forceMetadata) throws IOException {
        if (logChannel != null) {
            flushAndForceWrite(logChannel, forceMetadata);
            log.debug().attr("logId", () -> logChannel.getLogId()).log("Flush and sync current entry logger");
        }
    }

    void flushAndForceWrite(BufferedLogChannel logChannel, boolean forceMetadata) throws IOException {
        try {
            logChannel.flushAndForceWrite(forceMetadata);
        } catch (EntryLogWriteException e) {
            throw e;
        } catch (IOException e) {
            throw new EntryLogWriteException("Failed to flush entry log " + logChannel.getLogId(), e);
        }
    }

    void flushAndForceWriteIfRegularFlush(BufferedLogChannel logChannel, boolean forceMetadata) throws IOException {
        try {
            logChannel.flushAndForceWriteIfRegularFlush(forceMetadata);
        } catch (EntryLogWriteException e) {
            throw e;
        } catch (IOException e) {
            throw new EntryLogWriteException("Failed to flush entry log " + logChannel.getLogId(), e);
        }
    }

    /*
     * Creates a new log file. This method should be guarded by a lock,
     * so callers of this method should be in right scope of the lock.
     */
    @VisibleForTesting
    void createNewLog(long ledgerId) throws IOException {
        createNewLog(ledgerId, "");
    }

    void createNewLog(long ledgerId, String reason) throws IOException {
        if (ledgerId != UNASSIGNED_LEDGERID) {
            log.info()
                    .attr("ledgerId", ledgerId)
                    .attr("reason", reason)
                    .log("Creating a new entry log file for ledger");
        } else {
            log.info().attr("reason", reason).log("Creating a new entry log file");
        }

        BufferedLogChannel logChannel = getCurrentLogForLedger(ledgerId);
        // first tried to create a new log channel. add current log channel to ToFlush list only when
        // there is a new log channel. it would prevent that a log channel is referenced by both
        // *logChannel* and *ToFlush* list.
        if (null != logChannel) {

            // flush the internal buffer back to filesystem but not sync disk
            try {
                logChannel.flush();

                // Append ledgers map at the end of entry log
                logChannel.appendLedgersMap();
            } catch (EntryLogWriteException e) {
                throw e;
            } catch (IOException e) {
                throw new EntryLogWriteException(
                        "Failed to rotate entry log " + logChannel.getLogId() + " for ledger " + ledgerId, e);
            }

            File dirForNextEntryLog = selectDirForNextEntryLog();
            BufferedLogChannel newLogChannel;
            try {
                newLogChannel = entryLoggerAllocator.createNewLog(dirForNextEntryLog);
            } catch (EntryLogWriteException e) {
                throw e;
            } catch (IOException e) {
                throw new EntryLogWriteException("Failed to create a new entry log for ledger " + ledgerId, e);
            }
            entryLoggerAllocator.setWritingLogId(newLogChannel.getLogId());
            setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);
            log.info()
                    .attr("logId", logChannel.getLogId())
                    .attr("rotatedLogChannels", rotatedLogChannels)
                .log("Flushing entry logger back to filesystem, pending for syncing entry loggers");
            for (EntryLogListener listener : listeners) {
                listener.onRotateEntryLog();
            }
        } else {
            File dirForNextEntryLog = selectDirForNextEntryLog();
            BufferedLogChannel newLogChannel;
            try {
                newLogChannel = entryLoggerAllocator.createNewLog(dirForNextEntryLog);
            } catch (EntryLogWriteException e) {
                throw e;
            } catch (IOException e) {
                throw new EntryLogWriteException("Failed to create a new entry log for ledger " + ledgerId, e);
            }
            entryLoggerAllocator.setWritingLogId(newLogChannel.getLogId());
            setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);
        }
    }

    File selectDirForNextEntryLog() throws NoWritableLedgerDirException {
        return getDirForNextEntryLog(ledgerDirsManager.getWritableLedgerDirsForNewLog());
    }
}
