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
package org.apache.bookkeeper.proto;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol.ParsedAddRequest;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

/**
 * Processes add entry requests.
 */
@CustomLog
class WriteEntryProcessor extends PacketProcessorBase<ParsedAddRequest> implements WriteCallback {

    long startTimeNanos;

    @Override
    protected void reset() {
        super.reset();
        startTimeNanos = -1L;
    }

    public static WriteEntryProcessor create(ParsedAddRequest request, BookieRequestHandler requestHandler,
                                             BookieRequestProcessor requestProcessor) {
        WriteEntryProcessor wep = RECYCLER.get();
        wep.init(request, requestHandler, requestProcessor);
        requestProcessor.onAddRequestStart(requestHandler.ctx().channel());
        return wep;
    }

    @Override
    protected void processPacket() {
        if (requestProcessor.getBookie().isReadOnly()
            && !(request.isHighPriority() && requestProcessor.getBookie().isAvailableForHighPriorityWrites())) {
            log.warn("BookieServer is running in readonly mode,"
                    + " so rejecting the request from the client!");
            sendWriteReqResponse(BookieProtocol.EREADONLY,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EREADONLY, request),
                         requestProcessor.getRequestStats().getAddRequestStats());
            request.release();
            request.recycle();
            recycle();
            return;
        }

        startTimeNanos = MathUtils.nowInNano();
        int rc = BookieProtocol.EOK;
        ByteBuf addData = request.getData();
        try {
            if (request.isRecoveryAdd()) {
                requestProcessor.getBookie().recoveryAddEntry(addData, this, requestHandler, request.getMasterKey());
            } else {
                requestProcessor.getBookie().addEntry(addData, false, this,
                        requestHandler, request.getMasterKey());
            }
        } catch (OperationRejectedException e) {
            requestProcessor.getRequestStats().getAddEntryRejectedCounter().inc();
            // Avoid to log each occurrence of this exception as this can happen when the ledger storage is
            // unable to keep up with the write rate.
            log.debug()
                    .exception(e)
                    .attr("request", request)
                    .log("Operation rejected while writing");
            rc = BookieProtocol.ETOOMANYREQUESTS;
        } catch (IOException e) {
            log.error()
                    .exception(e)
                    .attr("request", request)
                    .log("Error writing");
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException | BookieException.LedgerFencedAndDeletedException lfe) {
            log.warn().attr("ledgerId", request.getLedgerId())
                    .attr("clientAddress", requestHandler.ctx().channel().remoteAddress())
                    .log("Write attempt on fenced/deleted ledger");
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            log.error()
                    .exception(e)
                    .attr("ledgerId", request.getLedgerId())
                    .log("Unauthorized access to ledger");
            rc = BookieProtocol.EUA;
        } catch (Throwable t) {
            log.error()
                    .exception(t)
                    .attr("ledgerId", request.ledgerId)
                    .attr("entryId", request.entryId)
                    .log("Unexpected exception while writing");
            // some bad request which cause unexpected exception
            rc = BookieProtocol.EBADREQ;
        }

        if (rc != BookieProtocol.EOK) {
            requestProcessor.getRequestStats().getAddEntryStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            sendWriteReqResponse(rc,
                         ResponseBuilder.buildErrorResponse(rc, request),
                         requestProcessor.getRequestStats().getAddRequestStats());
            request.recycle();
            recycle();
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                              BookieId addr, Object ctx) {
        if (BookieProtocol.EOK == rc) {
            requestProcessor.getRequestStats().getAddEntryStats()
                .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.getRequestStats().getAddEntryStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }

        requestHandler.prepareSendResponseV2(rc, request);
        requestProcessor.onAddRequestFinish();

        request.recycle();
        recycle();
    }

    @Override
    public String toString() {
        return String.format("WriteEntry(%d, %d)",
                             request.getLedgerId(), request.getEntryId());
    }

    @VisibleForTesting
    void recycle() {
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
