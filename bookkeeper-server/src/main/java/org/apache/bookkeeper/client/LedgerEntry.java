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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.InputStream;

/**
 * Ledger entry. Its a simple tuple containing the ledger id, the entry-id, and
 * the entry content.
 *
 */
public class LedgerEntry {
    long ledgerId;
    long entryId;
    long length;
    ByteBuf data;

    LedgerEntry(long lId, long eId) {
        this.ledgerId = lId;
        this.entryId = eId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public long getLength() {
        return length;
    }

    public byte[] getEntry() {
        byte[] entry = new byte[data.readableBytes()];
        data.readBytes(entry);
        data.release();
        return entry;
    }

    public InputStream getEntryInputStream() {
        return new ByteBufInputStream(data);
    }

    /**
     * Return the internal buffer that contains the entry payload.
     * <p>
     *
     * Note: It is responsibility of the caller to ensure to release the buffer after usage.
     */
    public ByteBuf getEntryBuffer() {
        return data;
    }
}
