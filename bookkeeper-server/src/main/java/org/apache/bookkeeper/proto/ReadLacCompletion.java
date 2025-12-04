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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;

class ReadLacCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.ReadLacCallback cb;

    public ReadLacCompletion(final CompletionKey key,
                             BookkeeperInternalCallbacks.ReadLacCallback originalCallback,
                             final Object ctx, final long ledgerId,
                             PerChannelBookieClient perChannelBookieClient) {
        super("ReadLAC", ctx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.readLacOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.readLacTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.ReadLacCallback() {
            @Override
            public void readLacComplete(int rc, long ledgerId,
                                        ByteBuf lacBuffer,
                                        ByteBuf lastEntryBuffer,
                                        Object ctx) {
                logOpResult(rc);
                originalCallback.readLacComplete(
                        rc, ledgerId, lacBuffer, lastEntryBuffer, ctx);
                key.release();
            }
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(
                () -> cb.readLacComplete(rc, ledgerId, null, null, ctx));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.ReadLacResponse readLacResponse = response.getReadLacResponse();
        ByteBuf lacBuffer = Unpooled.EMPTY_BUFFER;
        ByteBuf lastEntryBuffer = Unpooled.EMPTY_BUFFER;
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? readLacResponse.getStatus() : response.getStatus();

        if (readLacResponse.hasLacBody()) {
            lacBuffer = Unpooled.wrappedBuffer(readLacResponse.getLacBody().asReadOnlyByteBuffer());
        }

        if (readLacResponse.hasLastEntryBody()) {
            lastEntryBuffer = Unpooled.wrappedBuffer(readLacResponse.getLastEntryBody().asReadOnlyByteBuffer());
        }

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledgerId", ledgerId);
        }

        int rc = convertStatus(status, BKException.Code.ReadException);
        cb.readLacComplete(rc, ledgerId, lacBuffer.slice(),
                lastEntryBuffer.slice(), ctx);
    }
}