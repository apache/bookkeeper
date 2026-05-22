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
import java.io.EOFException;
import java.io.IOException;

/**
 * Interface for reading from a bookkeeper entry log.
 */
public interface LogReader extends AutoCloseable {
    /**
     * @return the id of the log being read from.
     */
    int logId();

    /**
     * @return the maximum offset in the file that can be read from.
     */
    long maxOffset();

    /**
     * Read a buffer from the file. It is the responsibility of the caller to release
     * the returned buffer.
     * @param offset the offset to read at
     * @param size the number of bytes to read
     * @return a bytebuf. The caller must release.
     */
    ByteBuf readBufferAt(long offset, int size) throws IOException, EOFException;

    void readIntoBufferAt(ByteBuf buffer, long offset, int size) throws IOException, EOFException;

    /**
     * Read an integer at a given offset.
     * @param offset the offset to read from.
     * @return the integer at that offset.
     */
    int readIntAt(long offset) throws IOException, EOFException;

    /**
     * Read an long at a given offset.
     * @param offset the offset to read from.
     * @return the long at that offset.
     */
    long readLongAt(long offset) throws IOException, EOFException;

    /**
     * Read an entry at a given offset.
     * The size of the entry must be at (offset - Integer.BYTES).
     * The payload of the entry starts at offset.
     * It is the responsibility of the caller to release the returned buffer.
     * @param offset the offset at which to read the entry.
     * @return a bytebuf. The caller must release.
     */
    ByteBuf readEntryAt(int offset) throws IOException, EOFException;

    @Override
    void close() throws IOException;

    boolean isClosed();
}
