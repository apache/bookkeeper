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
package org.apache.bookkeeper.client.api;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * An entry in a ledger.
 *
 * <p>The entry implementation may hold references to byte buffers under the hood. The users holding the
 * references to the instances of this class, are responsible for calling {@link LedgerEntry#close()} to release
 * resources held by the entry instances.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface LedgerEntry extends AutoCloseable {

    /**
     * The id of the ledger which contains the entry.
     *
     * @return the id of the ledger
     */
    long getLedgerId();

    /**
     * The id of the entry.
     *
     * @return the id of the entry
     */
    long getEntryId();

    /**
     * The length of the entry, that is the size of the content expressed in bytes.
     *
     * @return the size of the content
     */
    long getLength();

    /**
     * Returns the content of the entry as a byte array.
     *
     * @return the content of the entry
     */
    byte[] getEntryBytes();

    /**
     * Exposes this entry's data as an NIO {@link ByteBuffer}. The returned buffer
     * shares the content with this underneath bytebuf (which you can get it by {@link #getEntryBuffer()}).
     * Changing the position and limit of the returned NIO buffer does not affect the indexes and
     * marks of this underneath buffer.  This method is identical
     * to {@code entry.getEntryBuffer().nioBuffer()}. This method does not
     * modify {@code readerIndex} or {@code writerIndex} of the underlying bytebuf.
     */
    ByteBuffer getEntryNioBuffer();

    /**
     * Return the internal {@link ByteBuf} that contains the entry payload.
     *
     * <p>This call doesn't change the reference count on the returned bytebuf. If you want to use the bytebuf
     * after the entry is released (via {@link #close()}, the caller must retain the references of the bytebuf.
     *
     * @return a ByteBuf which contains the data
     */
    ByteBuf getEntryBuffer();

    /**
     * Returns a duplicate of this entry.
     *
     * <p>This call will retain a slice of the underneath byte buffer.
     *
     * @return a duplicated ledger entry
     */
    LedgerEntry duplicate();

    /**
     * {@inheritDoc}
     */
    @Override
    void close();

}
