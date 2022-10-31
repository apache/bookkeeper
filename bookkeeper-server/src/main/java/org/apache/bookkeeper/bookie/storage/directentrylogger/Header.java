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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;

/**
 * The 1K block at the head of the entry logger file
 * that contains the fingerprint and meta-data.
 *
 * <pre>
 * Header is composed of:
 * Fingerprint: 4 bytes "BKLO"
 * Log file HeaderVersion enum: 4 bytes
 * Ledger map offset: 8 bytes
 * Ledgers Count: 4 bytes
 * </pre>
 */
class Header {
    static final int LOGFILE_LEGACY_HEADER_SIZE = 1024;
    static final int LOGFILE_DIRECT_HEADER_SIZE = Buffer.ALIGNMENT;
    static final int HEADER_VERSION_OFFSET = 4;
    static final int LEDGERS_MAP_OFFSET = HEADER_VERSION_OFFSET + Integer.BYTES;
    static final int LEDGER_COUNT_OFFSET = LEDGERS_MAP_OFFSET + Long.BYTES;
    static final int HEADER_V0 = 0; // Old log file format (no ledgers map index)
    static final int HEADER_V1 = 1; // Introduced ledger map index
    static final int HEADER_CURRENT_VERSION = HEADER_V1;

    static final byte[] EMPTY_HEADER = new byte[LOGFILE_DIRECT_HEADER_SIZE];
    static {
        ByteBuf buf = Unpooled.wrappedBuffer(EMPTY_HEADER);
        buf.setByte(0, 'B');
        buf.setByte(1, 'K');
        buf.setByte(2, 'L');
        buf.setByte(3, 'O');
        buf.setInt(HEADER_VERSION_OFFSET, HEADER_V1);
        // legacy header size is 1024, while direct is 4096 so that it can be written as a single block
        // to avoid legacy failing when it encounters the header in direct, create a dummy entry, which
        // skips to the start of the second block
        buf.setInt(LOGFILE_LEGACY_HEADER_SIZE, (buf.capacity() - LOGFILE_LEGACY_HEADER_SIZE) - Integer.BYTES);
        buf.setLong(LOGFILE_LEGACY_HEADER_SIZE + Integer.BYTES, LogMetadata.INVALID_LID);
    };
    static int extractVersion(ByteBuf header) throws IOException {
        assertFingerPrint(header);
        return header.getInt(HEADER_VERSION_OFFSET);
    }

    static long extractLedgerMapOffset(ByteBuf header) throws IOException {
        assertFingerPrint(header);
        return header.getLong(LEDGERS_MAP_OFFSET);
    }

    static int extractLedgerCount(ByteBuf header) throws IOException {
        assertFingerPrint(header);
        return header.getInt(LEDGER_COUNT_OFFSET);
    }

    static void assertFingerPrint(ByteBuf header) throws IOException {
        if (header.getByte(0) != 'B'
            || header.getByte(1) != 'K'
            || header.getByte(2) != 'L'
            || header.getByte(3) != 'O') {
            throw new IOException(exMsg("Bad fingerprint (should be BKLO)")
                                  .kv("byte0", header.getByte(0))
                                  .kv("byte1", header.getByte(1))
                                  .kv("byte2", header.getByte(2))
                                  .kv("byte3", header.getByte(3))
                                  .toString());
        }
    }

    static void writeEmptyHeader(ByteBuf header) throws IOException {
        header.writeBytes(EMPTY_HEADER);
    }

    static void writeHeader(ByteBuf header,
                            long ledgerMapOffset, int ledgerCount) throws IOException {
        header.writeBytes(EMPTY_HEADER);
        header.setLong(LEDGERS_MAP_OFFSET, ledgerMapOffset);
        header.setInt(LEDGER_COUNT_OFFSET, ledgerCount);
    }

}
