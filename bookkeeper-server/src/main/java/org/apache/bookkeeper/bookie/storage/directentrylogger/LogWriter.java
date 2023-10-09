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
import java.io.IOException;

/**
 * Interface for writing data to a bookkeeper entry log.
 */
interface LogWriter extends AutoCloseable {
    /**
     * Return the ID of the log being written.
     */
    int logId();

    /**
     * Write the contents of a buffer at a predefined position in the log.
     * Both the position and the size of the buffer must be page aligned (i.e. to 4096).
     */
    void writeAt(long offset, ByteBuf buf) throws IOException;

    /**
     * Write a delimited buffer the log. The size of the buffer is first
     * written and then the buffer itself.
     * Note that the returned offset is for the buffer itself, not the size.
     * So, if a buffer is written at the start of the file, the returned offset
     * will be 4, not 0.
     * The returned offset is an int. Consequently, entries can only be written
     * in the first Integer.MAX_VALUE bytes of the file. This is due to how
     * offsets are stored in the index.
     *
     * @return the offset of the buffer within the file.
     */
    int writeDelimited(ByteBuf buf) throws IOException;

    /**
     * @return the number of bytes consumed by the buffer when written with #writeDelimited
     */
    int serializedSize(ByteBuf buf);

    /**
     * The current offset within the log at which the next call to #writeDelimited will
     * start writing.
     */
    long position() throws IOException;

    /**
     * Set the offset within the log at which the next call to #writeDelimited will start writing.
     */
    void position(long offset) throws IOException;

    /**
     * Flush all buffered writes to disk. This call must ensure that the bytes are actually on
     * disk before returning.
     */
    void flush() throws IOException;

    /**
     * Close any held resources.
     */
    void close() throws IOException;
}
