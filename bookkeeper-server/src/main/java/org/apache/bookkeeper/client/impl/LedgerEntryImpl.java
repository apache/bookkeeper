/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import org.apache.bookkeeper.client.api.LedgerEntry;

/**
 * Ledger entry. Its a simple tuple containing the ledger id, the entry-id, and
 * the entry content.
 */
public class LedgerEntryImpl implements LedgerEntry {

    private static final Recycler<LedgerEntryImpl> RECYCLER = new Recycler<LedgerEntryImpl>() {
        @Override
        protected LedgerEntryImpl newObject(Handle<LedgerEntryImpl> handle) {
            return new LedgerEntryImpl(handle);
        }
    };

    public static LedgerEntryImpl create(long ledgerId,
                                         long entryId) {
        LedgerEntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        return entry;
    }

    public static LedgerEntryImpl create(long ledgerId,
                                         long entryId,
                                         long length,
                                         ByteBuf buf) {
        LedgerEntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.length = length;
        entry.entryBuf = buf;
        return entry;
    }

    public static LedgerEntryImpl duplicate(LedgerEntry entry) {
        return create(
            entry.getLedgerId(),
            entry.getEntryId(),
            entry.getLength(),
            entry.getEntryBuffer().retainedSlice());
    }

    private final Handle<LedgerEntryImpl> recycleHandle;
    private long ledgerId;
    private long entryId;
    private long length;
    private ByteBuf entryBuf;

    private LedgerEntryImpl(Handle<LedgerEntryImpl> handle) {
        this.recycleHandle = handle;
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void setEntryBuf(ByteBuf buf) {
        ReferenceCountUtil.release(entryBuf);
        this.entryBuf = buf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getEntryId() {
        return entryId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLength() {
        return length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getEntryBytes() {
        return ByteBufUtil.getBytes(entryBuf, entryBuf.readerIndex(), entryBuf.readableBytes(), false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuf getEntryBuffer() {
        return entryBuf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getEntryNioBuffer() {
        return entryBuf.nioBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LedgerEntryImpl duplicate() {
        return duplicate(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        recycle();
    }

    private void recycle() {
        this.ledgerId = -1L;
        this.entryId = -1L;
        this.length = -1L;
        ReferenceCountUtil.release(entryBuf);
        this.entryBuf = null;
        recycleHandle.recycle(this);
    }
}
