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

import static org.apache.bookkeeper.bookie.EntryLogger.UNASSIGNED_LEDGERID;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;

@Slf4j
abstract class EntryLogManagerBase implements EntryLogManager {
    volatile List<BufferedLogChannel> rotatedLogChannels;
    final EntryLoggerAllocator entryLoggerAllocator;
    final LedgerDirsManager ledgerDirsManager;
    private final List<EntryLogger.EntryLogListener> listeners;
    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;

    EntryLogManagerBase(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<EntryLogger.EntryLogListener> listeners) {
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
        logChannel.write(sizeBuffer);

        long pos = logChannel.position();
        logChannel.write(entry);
        logChannel.registerWrittenEntry(ledger, entrySize);

        return (logChannel.getLogId() << 32L) | pos;
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
            logChannel.flushAndForceWrite(forceMetadata);
            log.debug("Flush and sync current entry logger {}", logChannel.getLogId());
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
            log.info("Creating a new entry log file for ledger '{}' {}", ledgerId, reason);
        } else {
            log.info("Creating a new entry log file {}", reason);
        }

        BufferedLogChannel logChannel = getCurrentLogForLedger(ledgerId);
        // first tried to create a new log channel. add current log channel to ToFlush list only when
        // there is a new log channel. it would prevent that a log channel is referenced by both
        // *logChannel* and *ToFlush* list.
        if (null != logChannel) {

            // flush the internal buffer back to filesystem but not sync disk
            logChannel.flush();

            // Append ledgers map at the end of entry log
            logChannel.appendLedgersMap();

            BufferedLogChannel newLogChannel = entryLoggerAllocator.createNewLog(selectDirForNextEntryLog());
            setCurrentLogForLedgerAndAddToRotate(ledgerId, newLogChannel);
            log.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                    logChannel.getLogId(), rotatedLogChannels);
            for (EntryLogListener listener : listeners) {
                listener.onRotateEntryLog();
            }
        } else {
            setCurrentLogForLedgerAndAddToRotate(ledgerId,
                    entryLoggerAllocator.createNewLog(selectDirForNextEntryLog()));
        }
    }

    File selectDirForNextEntryLog() throws NoWritableLedgerDirException {
        return getDirForNextEntryLog(ledgerDirsManager.getWritableLedgerDirsForNewLog());
    }
}
