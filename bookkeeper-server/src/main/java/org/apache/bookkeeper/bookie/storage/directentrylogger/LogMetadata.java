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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.BiConsumerLong;

class LogMetadata {

    /**
     * Ledgers map is composed of multiple parts that can be split into separated entries. Each of them is composed of:
     *
     * <pre>
     * length: (4 bytes) [0-3]
     * ledger id (-1): (8 bytes) [4 - 11]
     * entry id: (8 bytes) [12-19]
     * num ledgers stored in current metadata entry: (4 bytes) [20 - 23]
     * ledger entries: sequence of (ledgerid, size) (8 + 8 bytes each) [24..]
     * </pre>
     */
    static final int LEDGERS_MAP_HEADER_SIZE = 4 + 8 + 8 + 4;
    static final int LEDGERS_MAP_ENTRY_SIZE = 8 + 8;

    // Break the ledgers map into multiple batches, each of which can contain up to 10K ledgers
    static final int LEDGERS_MAP_MAX_BATCH_SIZE = 10000;
    static final int LEDGERS_MAP_MAX_MAP_SIZE =
        LEDGERS_MAP_HEADER_SIZE + LEDGERS_MAP_ENTRY_SIZE * LEDGERS_MAP_MAX_BATCH_SIZE;

    static final long INVALID_LID = -1L;
    // EntryId used to mark an entry (belonging to INVALID_ID)
    // as a component of the serialized ledgers map
    static final long LEDGERS_MAP_ENTRY_ID = -2L;

    static void write(LogWriter writer,
                      EntryLogMetadata metadata,
                      ByteBufAllocator allocator)
            throws IOException {
        long ledgerMapOffset = writer.position();
        ConcurrentLongLongHashMap ledgersMap = metadata.getLedgersMap();
        int numberOfLedgers = (int) ledgersMap.size();

        // Write the ledgers map into several batches
        final ByteBuf serializedMap = allocator.buffer(LEDGERS_MAP_MAX_BATCH_SIZE);
        BiConsumerLong writingConsumer = new BiConsumerLong() {
                int remainingLedgers = numberOfLedgers;
                boolean startNewBatch = true;
                int remainingInBatch = 0;

                @Override
                public void accept(long ledgerId, long size) {
                    if (startNewBatch) {
                        int batchSize = Math.min(remainingLedgers, LEDGERS_MAP_MAX_BATCH_SIZE);
                        serializedMap.clear();
                        serializedMap.writeLong(INVALID_LID);
                        serializedMap.writeLong(LEDGERS_MAP_ENTRY_ID);
                        serializedMap.writeInt(batchSize);

                        startNewBatch = false;
                        remainingInBatch = batchSize;
                    }
                    // Dump the ledger in the current batch
                    serializedMap.writeLong(ledgerId);
                    serializedMap.writeLong(size);
                    --remainingLedgers;

                    if (--remainingInBatch == 0) {
                        // Close current batch
                        try {
                            writer.writeDelimited(serializedMap);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        startNewBatch = true;
                    }
                }
            };
        try {
            ledgersMap.forEach(writingConsumer);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw e;
            }
        } finally {
            ReferenceCountUtil.release(serializedMap);
        }
        ByteBuf buf = allocator.buffer(Buffer.ALIGNMENT);
        try {
            Header.writeHeader(buf, ledgerMapOffset, numberOfLedgers);
            writer.writeAt(0, buf);
        } finally {
            ReferenceCountUtil.release(buf);
        }
        writer.flush();
    }

    static EntryLogMetadata read(LogReader reader) throws IOException {
        ByteBuf header = reader.readBufferAt(0, Header.LOGFILE_LEGACY_HEADER_SIZE);
        try {
            int headerVersion = Header.extractVersion(header);
            if (headerVersion < Header.HEADER_V1) {
                throw new IOException(exMsg("Old log file header").kv("headerVersion", headerVersion).toString());
            }
            long ledgerMapOffset = Header.extractLedgerMapOffset(header);
            if (ledgerMapOffset > Integer.MAX_VALUE) {
                throw new IOException(exMsg("ledgerMapOffset too high").kv("ledgerMapOffset", ledgerMapOffset)
                                      .kv("maxOffset", Integer.MAX_VALUE).toString());
            }
            if (ledgerMapOffset <= 0) {
                throw new IOException(exMsg("ledgerMap never written").kv("ledgerMapOffset", ledgerMapOffset)
                                      .toString());
            }

            long offset = ledgerMapOffset;
            EntryLogMetadata meta = new EntryLogMetadata(reader.logId());
            while (offset < reader.maxOffset()) {
                int mapSize = reader.readIntAt((int) offset);
                if (mapSize >= LogMetadata.LEDGERS_MAP_MAX_MAP_SIZE) {
                    throw new IOException(exMsg("ledgerMap too large")
                                          .kv("maxSize", LogMetadata.LEDGERS_MAP_MAX_MAP_SIZE)
                                          .kv("mapSize", mapSize).toString());
                } else if (mapSize <= 0) {
                    break;
                }
                offset += Integer.BYTES;

                ByteBuf ledgerMapBuffer = reader.readBufferAt(offset, mapSize);
                try {
                    offset += mapSize;

                    long ledgerId = ledgerMapBuffer.readLong();
                    if (ledgerId != LogMetadata.INVALID_LID) {
                        throw new IOException(exMsg("Bad ledgerID").kv("ledgerId", ledgerId).toString());
                    }
                    long entryId = ledgerMapBuffer.readLong();
                    if (entryId != LogMetadata.LEDGERS_MAP_ENTRY_ID) {
                        throw new IOException(exMsg("Unexpected entry ID. Expected special value")
                                              .kv("entryIdRead", entryId)
                                              .kv("entryIdExpected", LogMetadata.LEDGERS_MAP_ENTRY_ID).toString());
                    }
                    int countInBatch = ledgerMapBuffer.readInt();
                    for (int i = 0; i < countInBatch; i++) {
                        ledgerId = ledgerMapBuffer.readLong();
                        long size = ledgerMapBuffer.readLong();
                        meta.addLedgerSize(ledgerId, size);
                    }
                    if (ledgerMapBuffer.isReadable()) {
                        throw new IOException(exMsg("ledgerMapSize didn't match content")
                                              .kv("expectedCount", countInBatch)
                                              .kv("bufferSize", mapSize)
                                              .kv("bytesRemaining", ledgerMapBuffer.readableBytes())
                                              .toString());
                    }
                } finally {
                    ReferenceCountUtil.release(ledgerMapBuffer);
                }
            }
            return meta;
        } catch (IOException ioe) {
            throw new IOException(exMsg("Error reading index").kv("logId", reader.logId())
                                  .kv("reason", ioe.getMessage()).toString(), ioe);
        } finally {
            ReferenceCountUtil.release(header);
        }
    }
}
