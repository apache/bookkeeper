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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol.ParsedAddRequest;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;

/**
 * Processes batched add entry requests.
 */
@Slf4j
public class WriteBatchEntryProcessor extends PacketProcessorBase<ParsedAddRequest> implements WriteCallback {
    long startTimeNanos;
    List<ParsedAddRequest> requests;
    AtomicInteger requestCount;

    @Override
    protected void reset() {
        requests = null;
        requestHandler = null;
        requestProcessor = null;
        startTimeNanos = -1L;
    }

    public static WriteBatchEntryProcessor create(List<ParsedAddRequest> requests, BookieRequestHandler requestHandler,
                                                  BookieRequestProcessor requestProcessor) {
        WriteBatchEntryProcessor wbep = RECYCLER.get();
        wbep.init(requests, requestHandler, requestProcessor);
        requestProcessor.onAddRequestStart(requestHandler.ctx().channel(), requests.size());
        return wbep;
    }

    protected void init(List<ParsedAddRequest> requests, BookieRequestHandler requestHandler,
                      BookieRequestProcessor requestProcessor) {
        this.requests = requests;
        this.requestHandler = requestHandler;
        this.requestProcessor = requestProcessor;
        this.enqueueNanos = MathUtils.nowInNano();
        this.requestCount = new AtomicInteger(requests.size());
    }

    @Override
    protected void processPacket() {

    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
        if (BookieProtocol.EOK == rc) {
            requestProcessor.getRequestStats().getAddEntryStats()
                .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.getRequestStats().getAddEntryStats()
                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }

        sendWriteReqResponse(rc,
            ResponseBuilder.buildAddResponse(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId),
            requestProcessor.getRequestStats().getAddRequestStats());

        if (requestCount.decrementAndGet() == 0) {
            recycle();
        }
    }

    @Override
    public void run() {
        if (requestProcessor.getBookie().isReadOnly()) {
                log.warn("BookieServer is running in readOnly mode, so rejecting the request from the client!");
                for (ParsedAddRequest r : requests) {
                    writeComplete(BookieProtocol.EREADONLY, r.getLedgerId(), r.getEntryId(), null,
                        requestHandler.ctx());
                    r.release();
                    r.recycle();
                }
                return;
        }

        startTimeNanos = MathUtils.nowInNano();
        int rc = BookieProtocol.EOK;
        try {
            requestProcessor.getBookie().addEntry(requests, false, this, requestHandler,
                requestProcessor.getRequestStats());
        } catch (Throwable t) {
            log.error("Unexpected exception while writing requests ", t);
            rc = BookieProtocol.EBADREQ;
        }

        if (rc != BookieProtocol.EOK) {
            requestProcessor.getRequestStats().getAddEntryStats()
            .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            for (ParsedAddRequest r : requests) {
                writeComplete(rc, r.getLedgerId(), r.getEntryId(), null, requestHandler.ctx());
                r.release();
                r.recycle();
            }
        }
    }

    void recycle() {
        reset();
        recyclerHandle.recycle(this);
    }

    private final Recycler.Handle<WriteBatchEntryProcessor> recyclerHandle;
    private WriteBatchEntryProcessor(Recycler.Handle<WriteBatchEntryProcessor> recycleHandle) {
        this.recyclerHandle = recycleHandle;
    }
    private static final Recycler<WriteBatchEntryProcessor> RECYCLER = new Recycler<WriteBatchEntryProcessor>() {
        @Override
        protected WriteBatchEntryProcessor newObject(Recycler.Handle<WriteBatchEntryProcessor> handle) {
            return new WriteBatchEntryProcessor(handle);
        }
    };
}
