/**
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
package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes add entry requests.
 */
class WriteEntryProcessor extends PacketProcessorBase implements WriteCallback {

    private static final Logger LOG = LoggerFactory.getLogger(WriteEntryProcessor.class);

    long startTimeNanos;

    protected void reset() {
        super.reset();
        startTimeNanos = -1L;
    }

    public static WriteEntryProcessor create(Request request, Channel channel,
                               BookieRequestProcessor requestProcessor) {
        WriteEntryProcessor wep = RECYCLER.get();
        wep.init(request, channel, requestProcessor);
        return wep;
    }

    @Override
    protected void processPacket() {
        assert (request instanceof BookieProtocol.AddRequest);
        BookieProtocol.AddRequest add = (BookieProtocol.AddRequest) request;

        if (requestProcessor.bookie.isReadOnly()) {
            LOG.warn("BookieServer is running in readonly mode,"
                    + " so rejecting the request from the client!");
            sendResponse(BookieProtocol.EREADONLY,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EREADONLY, add),
                         requestProcessor.addRequestStats);
            add.release();
            add.recycle();
            return;
        }

        startTimeNanos = MathUtils.nowInNano();
        int rc = BookieProtocol.EOK;
        ByteBuf addData = add.getData();
        try {
            if (add.isRecoveryAdd()) {
                requestProcessor.bookie.recoveryAddEntry(addData, this, channel, add.getMasterKey());
            } else {
                requestProcessor.bookie.addEntry(addData, this, channel, add.getMasterKey());
            }
        } catch (IOException e) {
            LOG.error("Error writing " + add, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException lfe) {
            LOG.error("Attempt to write to fenced ledger", lfe);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + add.getLedgerId(), e);
            rc = BookieProtocol.EUA;
        } catch (Throwable t) {
            LOG.error("Unexpected exception while writing {}@{} : {}", add.ledgerId, add.entryId, t.getMessage(), t);
            // some bad request which cause unexpected exception
            rc = BookieProtocol.EBADREQ;
        } finally {
            addData.release();
        }

        if (rc != BookieProtocol.EOK) {
            requestProcessor.addEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(rc,
                         ResponseBuilder.buildErrorResponse(rc, add),
                         requestProcessor.addRequestStats);
            add.recycle();
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                              BookieSocketAddress addr, Object ctx) {
        if (BookieProtocol.EOK == rc) {
            requestProcessor.addEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.addEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
        }
        sendResponse(rc,
                     ResponseBuilder.buildAddResponse(request),
                     requestProcessor.addRequestStats);
        request.recycle();
        recycle();
    }

    @Override
    public String toString() {
        return String.format("WriteEntry(%d, %d)",
                             request.getLedgerId(), request.getEntryId());
    }

    private void recycle() {
        reset();
        recyclerHandle.recycle(this);
    }

    private final Recycler.Handle<WriteEntryProcessor> recyclerHandle;

    private WriteEntryProcessor(Recycler.Handle<WriteEntryProcessor> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<WriteEntryProcessor> RECYCLER = new Recycler<WriteEntryProcessor>() {
        @Override
        protected WriteEntryProcessor newObject(Recycler.Handle<WriteEntryProcessor> handle) {
            return new WriteEntryProcessor(handle);
        }
    };
}
