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

import static org.apache.bookkeeper.bookie.EntryLogger.INVALID_LID;
import static org.apache.bookkeeper.bookie.EntryLogger.UNASSIGNED_LEDGERID;

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;

@Slf4j
class EntryLogManagerForSingleEntryLog extends EntryLogManagerBase {

    private volatile BufferedLogChannel activeLogChannel;
    private long logIdBeforeFlush = INVALID_LID;
    private final AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);
    private EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;

    EntryLogManagerForSingleEntryLog(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<EntryLogger.EntryLogListener> listeners,
            EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus) {
        super(conf, ledgerDirsManager, entryLoggerAllocator, listeners);
        this.rotatedLogChannels = new LinkedList<BufferedLogChannel>();
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        // Register listener for disk full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new
                // entry log.
                BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                if (currentActiveLogChannel != null
                        && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskAlmostFull(File disk) {
                // If the current entry log disk is almost full, then create new entry
                // log.
                BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                if (currentActiveLogChannel != null
                        && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }
        };
    }

    @Override
    public synchronized long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        return super.addEntry(ledger, entry, rollLog);
    }

    @Override
    synchronized BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize,
            boolean rollLog) throws IOException {
        if (null == activeLogChannel) {
            // log channel can be null because the file is deferred to be created
            createNewLog(UNASSIGNED_LEDGERID, "because current active log channel has not initialized yet");
        }

        boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(activeLogChannel, entrySize)
                : readEntryLogHardLimit(activeLogChannel, entrySize);
        // Create new log if logSizeLimit reached or current disk is full
        boolean createNewLog = shouldCreateNewEntryLog.get();
        if (createNewLog || reachEntryLogLimit) {
            if (activeLogChannel != null) {
                activeLogChannel.flushAndForceWriteIfRegularFlush(false);
            }
            createNewLog(UNASSIGNED_LEDGERID,
                ": createNewLog = " + createNewLog + ", reachEntryLogLimit = " + reachEntryLogLimit);
            // Reset the flag
            if (createNewLog) {
                shouldCreateNewEntryLog.set(false);
            }
        }
        return activeLogChannel;
    }

    @Override
    synchronized void createNewLog(long ledgerId) throws IOException {
        super.createNewLog(ledgerId);
    }

    @Override
    public synchronized void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) {
        BufferedLogChannel hasToRotateLogChannel = activeLogChannel;
        activeLogChannel = logChannel;
        if (hasToRotateLogChannel != null) {
            rotatedLogChannels.add(hasToRotateLogChannel);
        }
    }

    @Override
    public BufferedLogChannel getCurrentLogForLedger(long ledgerId) {
        return activeLogChannel;
    }

    @Override
    public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
        BufferedLogChannel activeLogChannelTemp = activeLogChannel;
        if ((activeLogChannelTemp != null) && (activeLogChannelTemp.getLogId() == entryLogId)) {
            return activeLogChannelTemp;
        }
        return null;
    }

    @Override
    public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
        Collections.shuffle(writableLedgerDirs);
        return writableLedgerDirs.get(0);
    }

    @Override
    public void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    public long getCurrentLogId() {
        BufferedLogChannel currentActiveLogChannel = activeLogChannel;
        if (currentActiveLogChannel != null) {
            return currentActiveLogChannel.getLogId();
        } else {
            return EntryLogger.UNINITIALIZED_LOG_ID;
        }
    }

    @Override
    public void flushCurrentLogs() throws IOException {
        BufferedLogChannel currentActiveLogChannel = activeLogChannel;
        if (currentActiveLogChannel != null) {
            /**
             * flushCurrentLogs method is called during checkpoint, so
             * metadata of the file also should be force written.
             */
            flushLogChannel(currentActiveLogChannel, true);
        }
    }

    @Override
    void flushRotatedLogs() throws IOException {
        List<BufferedLogChannel> channels = null;
        synchronized (this) {
            channels = rotatedLogChannels;
            rotatedLogChannels = new LinkedList<BufferedLogChannel>();
        }
        if (null == channels) {
            return;
        }
        Iterator<BufferedLogChannel> chIter = channels.iterator();
        while (chIter.hasNext()) {
            BufferedLogChannel channel = chIter.next();
            try {
                channel.flushAndForceWrite(true);
            } catch (IOException ioe) {
                // rescue from flush exception, add unflushed channels back
                synchronized (this) {
                    if (null == rotatedLogChannels) {
                        rotatedLogChannels = channels;
                    } else {
                        rotatedLogChannels.addAll(0, channels);
                    }
                }
                throw ioe;
            }
            // remove the channel from the list after it is successfully flushed
            chIter.remove();
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            channel.close();
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            log.info("Synced entry logger {} to disk.", channel.getLogId());
        }
    }

    @Override
    public void close() throws IOException {
        if (activeLogChannel != null) {
            activeLogChannel.close();
        }
    }

    @Override
    public void forceClose() {
        IOUtils.close(log, activeLogChannel);
    }

    @Override
    public void prepareEntryMemTableFlush() {
        logIdBeforeFlush = getCurrentLogId();
    }

    @Override
    public boolean commitEntryMemTableFlush() throws IOException {
        long logIdAfterFlush = getCurrentLogId();
        /*
         * in any case that an entry log reaches the limit, we roll the log
         * and start checkpointing. if a memory table is flushed spanning
         * over two entry log files, we also roll log. this is for
         * performance consideration: since we don't wanna checkpoint a new
         * log file that ledger storage is writing to.
         */
        if (reachEntryLogLimit(activeLogChannel, 0L) || logIdAfterFlush != logIdBeforeFlush) {
            log.info("Rolling entry logger since it reached size limitation");
            createNewLog(UNASSIGNED_LEDGERID,
                "due to reaching log limit after flushing memtable : logIdBeforeFlush = "
                    + logIdBeforeFlush + ", logIdAfterFlush = " + logIdAfterFlush);
            return true;
        }
        return false;
    }

    @Override
    public void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException{
        if (numBytesFlushed > 0) {
            // if bytes are added between previous flush and this checkpoint,
            // it means bytes might live at current active entry log, we need
            // roll current entry log and then issue checkpoint to underlying
            // interleaved ledger storage.
            createNewLog(UNASSIGNED_LEDGERID,
                "due to preparing checkpoint : numBytesFlushed = " + numBytesFlushed);
        }
    }

    @Override
    public EntryLogger.BufferedLogChannel createNewLogForCompaction() throws IOException {
        return entryLoggerAllocator.createNewLogForCompaction(selectDirForNextEntryLog());
    }
}
