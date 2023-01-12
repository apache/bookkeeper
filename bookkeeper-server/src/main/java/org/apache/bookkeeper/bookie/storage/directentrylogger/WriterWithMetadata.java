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

import static com.google.common.base.Preconditions.checkState;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import org.apache.bookkeeper.bookie.EntryLogMetadata;

/**
 * WriterWithMetadata.
 */
class WriterWithMetadata {
    private final LogWriter writer;
    private final EntryLogMetadata metadata;
    private final ByteBufAllocator allocator;

    WriterWithMetadata(LogWriter writer, EntryLogMetadata metadata,
                       ByteBufAllocator allocator) throws IOException {
        this.writer = writer;
        this.metadata = metadata;
        this.allocator = allocator;

        ByteBuf buf = allocator.buffer(Buffer.ALIGNMENT);
        try {
            Header.writeEmptyHeader(buf);
            writer.writeAt(0, buf);
            writer.position(buf.capacity());
        } finally {
            buf.release();
        }
    }

    int logId() {
        return writer.logId();
    }

    boolean shouldRoll(ByteBuf entry, long rollThreshold) throws IOException {
        return (writer.position() + writer.serializedSize(entry)) > rollThreshold;
    }

    long addEntry(long ledgerId, ByteBuf entry) throws IOException {
        int size = entry.readableBytes();
        metadata.addLedgerSize(ledgerId, size + Integer.BYTES);
        long offset = writer.writeDelimited(entry);
        checkState(offset < Integer.MAX_VALUE, "Offsets can't be higher than max int (%d)", offset);
        return ((long) writer.logId()) << 32 | offset;
    }

    void flush() throws IOException {
        writer.flush();
    }

    void finalizeAndClose() throws IOException {
        writer.flush();
        LogMetadata.write(writer, metadata, allocator);
        writer.close();
    }
}
