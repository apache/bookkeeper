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

package org.apache.bookkeeper.proto;

import io.netty.util.Recycler;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.net.BookieId;
import org.slf4j.MDC;

class AddCompletion extends CompletionValue implements BookkeeperInternalCallbacks.WriteCallback {

    static AddCompletion acquireAddCompletion(final CompletionKey key,
                                              final BookkeeperInternalCallbacks.WriteCallback originalCallback,
                                              final Object originalCtx,
                                              final long ledgerId, final long entryId,
                                              PerChannelBookieClient perChannelBookieClient) {
        AddCompletion completion = ADD_COMPLETION_RECYCLER.get();
        completion.reset(key, originalCallback, originalCtx, ledgerId, entryId, perChannelBookieClient);
        return completion;
    }

    final Recycler.Handle<AddCompletion> handle;

    CompletionKey key = null;
    BookkeeperInternalCallbacks.WriteCallback originalCallback = null;

    AddCompletion(Recycler.Handle<AddCompletion> handle) {
        super("Add", null, -1, -1, null);
        this.handle = handle;
    }

    void reset(final CompletionKey key,
               final BookkeeperInternalCallbacks.WriteCallback originalCallback,
               final Object originalCtx,
               final long ledgerId, final long entryId,
               PerChannelBookieClient perChannelBookieClient) {
        this.key = key;
        this.originalCallback = originalCallback;
        this.ctx = originalCtx;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.startTime = org.apache.bookkeeper.common.util.MathUtils.nowInNano();

        this.opLogger = perChannelBookieClient.addEntryOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.addTimeoutOpLogger;
        this.perChannelBookieClient = perChannelBookieClient;
        this.mdcContextMap = perChannelBookieClient.preserveMdcForTaskExecution ? MDC.getCopyOfContextMap() : null;
    }

    @Override
    public void release() {
        this.ctx = null;
        this.opLogger = null;
        this.timeoutOpLogger = null;
        this.perChannelBookieClient = null;
        this.mdcContextMap = null;
        handle.recycle(this);
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                              BookieId addr,
                              Object ctx) {
        logOpResult(rc);
        originalCallback.writeComplete(rc, ledgerId, entryId, addr, ctx);
        key.release();
        this.release();
    }

    @Override
    boolean maybeTimeout() {
        if (MathUtils.elapsedNanos(startTime) >= perChannelBookieClient.addEntryTimeoutNanos) {
            timeout();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(
                () -> writeComplete(rc, ledgerId, entryId, perChannelBookieClient.bookieId, ctx));
    }

    @Override
    public void setOutstanding() {
        perChannelBookieClient.addEntryOutstanding.inc();
    }

    @Override
    public void handleV2Response(
            long ledgerId, long entryId, BookkeeperProtocol.StatusCode status,
            BookieProtocol.Response response) {
        perChannelBookieClient.addEntryOutstanding.dec();
        handleResponse(ledgerId, entryId, status);
    }

    @Override
    public void handleV3Response(
            BookkeeperProtocol.Response response) {
        perChannelBookieClient.addEntryOutstanding.dec();
        BookkeeperProtocol.AddResponse addResponse = response.getAddResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? addResponse.getStatus() : response.getStatus();
        handleResponse(addResponse.getLedgerId(), addResponse.getEntryId(),
                status);
    }

    private void handleResponse(long ledgerId, long entryId,
                                BookkeeperProtocol.StatusCode status) {
        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledger", ledgerId, "entry", entryId);
        }

        int rc = convertStatus(status, BKException.Code.WriteException);
        writeComplete(rc, ledgerId, entryId, perChannelBookieClient.bookieId, ctx);
    }

    private static final Recycler<AddCompletion> ADD_COMPLETION_RECYCLER = new Recycler<AddCompletion>() {
        @Override
        protected AddCompletion newObject(Recycler.Handle<AddCompletion> handle) {
            return new AddCompletion(handle);
        }
    };
}
