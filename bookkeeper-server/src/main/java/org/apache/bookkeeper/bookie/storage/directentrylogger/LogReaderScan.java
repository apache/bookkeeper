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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;

import static org.apache.bookkeeper.bookie.storage.directentrylogger.LogMetadata.INVALID_LID;

class LogReaderScan {
    static void scan(ByteBufAllocator allocator, LogReader reader, EntryLogScanner scanner) throws IOException {
        int offset = Header.LOGFILE_LEGACY_HEADER_SIZE;

        ByteBuf entry = allocator.directBuffer(16 * 1024 * 1024);

        try {
            while (offset < reader.maxOffset()) {
                int initOffset = offset;
                int entrySize = reader.readIntAt(offset);
                if (entrySize < 0) { // padding, skip it
                    offset = Buffer.nextAlignment(offset);
                    continue;
                } else if (entrySize == 0) { // preallocated space, we're done
                    break;
                }

                // The 4 bytes for the entrySize need to be added only after we
                // have realigned on the block boundary.
                offset += Integer.BYTES;

                long ledgerId = reader.readLongAt(offset);
                if (ledgerId == INVALID_LID || !scanner.accept(ledgerId)) {
                    offset += entrySize;
                    continue;
                }

                entry.clear();
                switch (scanner.getReadLengthType()) {
                    case READ_NOTHING:
                        scanner.process(ledgerId, initOffset, entrySize);
                        break;
                    case READ_LEDGER_ENTRY_ID:
                        long entryId = reader.readLongAt(offset + Long.BYTES);
                        scanner.process(ledgerId, initOffset, entrySize, entryId);
                        break;
                    case READ_ALL:
                        reader.readIntoBufferAt(entry, offset, entrySize);
                        scanner.process(ledgerId, initOffset, entry);
                        break;
                    default:
                        throw new IOException("Unknown read length type: " + scanner.getReadLengthType());
                }
                offset += entrySize;
            }
        } finally {
            ReferenceCountUtil.release(entry);
        }
    }
}
