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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.BookieProtocol.ReadRequest;
import org.apache.bookkeeper.stats.OpStatsLogger;


@CustomLog
class ReadEntryProcessor extends PacketProcessorBase<ReadRequest> {

    protected ExecutorService fenceThreadPool;
    protected boolean throttleReadResponses;

    public static ReadEntryProcessor create(ReadRequest request,
                                            BookieRequestHandler requestHandler,
                                            BookieRequestProcessor requestProcessor,
                                            ExecutorService fenceThreadPool,
                                            boolean throttleReadResponses) {
        ReadEntryProcessor rep = RECYCLER.get();
        rep.init(request, requestHandler, requestProcessor);
        rep.fenceThreadPool = fenceThreadPool;
        rep.throttleReadResponses = throttleReadResponses;
        requestProcessor.onReadRequestStart(requestHandler.ctx().channel());
        return rep;
    }

    @Override
    protected void processPacket() {
        log.debug().attr("request", request).log("Received new read request");
        if (!requestHandler.ctx().channel().isOpen()) {
            log.debug().attr("channel", requestHandler.ctx().channel()).log("Dropping read request for closed channel");
            requestProcessor.onReadRequestFinish();
            recycle();
            return;
        }
        int errorCode = BookieProtocol.EOK;
        long startTimeNanos = MathUtils.nowInNano();
        ReferenceCounted data = null;
        try {
            CompletableFuture<Boolean> fenceResult = null;
            if (request.isFencing()) {
                log.warn().attr("ledgerId", request.getLedgerId())
                        .attr("fencedBy", requestHandler.ctx().channel().remoteAddress())
                        .log("Ledger fenced");

                if (request.hasMasterKey()) {
                    fenceResult = requestProcessor.getBookie().fenceLedger(request.getLedgerId(),
                            request.getMasterKey());
                } else {
                    log.error().attr("ledgerId", request.getLedgerId()).log("Password not provided, Not safe to fence");
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = readData();
            log.debug().attr("refCount", data.refCnt()).log("Read entry");
            if (fenceResult != null) {
                handleReadResultForFenceRead(fenceResult, data, startTimeNanos);
                return;
            }
        } catch (Bookie.NoLedgerException | BookieException.LedgerFencedAndDeletedException e) {
            log.debug()
                    .exception(e)
                    .attr("request", request)
                    .log("Error reading");
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            log.debug()
                    .exception(e)
                    .attr("request", request)
                    .log("Error reading");
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            log.debug()
                    .exception(e)
                    .attr("request", request)
                    .log("Error reading");
            errorCode = BookieProtocol.EIO;
        } catch (BookieException.DataUnknownException e) {
            log.error()
                    .exception(e)
                    .attr("ledgerId", request.getLedgerId())
                    .log("Ledger is in an unknown state");
            errorCode = BookieProtocol.EUNKNOWNLEDGERSTATE;
        } catch (BookieException e) {
            log.error()
                    .exception(e)
                    .attr("ledgerId", request.getLedgerId())
                    .log("Unauthorized access to ledger");
            errorCode = BookieProtocol.EUA;
        } catch (Throwable t) {
            log.error()
                    .exception(t)
                    .attr("ledgerId", request.getLedgerId())
                    .attr("entryId", request.getEntryId())
                    .log("Unexpected exception reading");
            errorCode = BookieProtocol.EBADREQ;
        }

        log.trace()
                .attr("rc", errorCode)
                .attr("request", request)
                .log("Read entry");
        sendResponse(data, errorCode, startTimeNanos);
    }

    protected ReferenceCounted readData() throws Exception {
        return requestProcessor.getBookie().readEntry(request.getLedgerId(), request.getEntryId());
    }

    private void sendResponse(ReferenceCounted data, int errorCode, long startTimeNanos) {
        final RequestStats stats = requestProcessor.getRequestStats();
        final OpStatsLogger logger = stats.getReadEntryStats();
        BookieProtocol.Response response;
        if (errorCode == BookieProtocol.EOK) {
            logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            response = buildReadResponse(data);
        } else {
            if (data != null) {
                ReferenceCountUtil.release(data);
            }
            logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            response = ResponseBuilder.buildErrorResponse(errorCode, request);
        }

        sendReadReqResponse(errorCode, response, stats.getReadRequestStats(), throttleReadResponses);
        recycle();
    }

    protected BookieProtocol.Response buildReadResponse(ReferenceCounted data) {
        return ResponseBuilder.buildReadResponse((ByteBuf) data, request);
    }

    private void sendFenceResponse(Boolean result, ReferenceCounted data, long startTimeNanos) {
        final int retCode = result != null && result ? BookieProtocol.EOK : BookieProtocol.EIO;
        sendResponse(data, retCode, startTimeNanos);
    }

    private void handleReadResultForFenceRead(CompletableFuture<Boolean> fenceResult,
                                              ReferenceCounted data,
                                              long startTimeNanos) {
        if (null != fenceThreadPool) {
            fenceResult.whenCompleteAsync(new FutureEventListener<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    sendFenceResponse(result, data, startTimeNanos);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error().exception(t).log("Error processing fence request");
                    // if failed to fence, fail the read request to make it retry.
                    sendResponse(data, BookieProtocol.EIO, startTimeNanos);
                }
            }, fenceThreadPool);
        } else {
            try {
                Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                sendFenceResponse(fenced, data, startTimeNanos);
                return;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error()
                        .exception(ie)
                        .attr("request", request)
                        .log("Interrupting fence read entry");
            } catch (ExecutionException ee) {
                log.error()
                        .exception(ee.getCause())
                        .attr("request", request)
                        .log("Failed to fence read entry");
            } catch (TimeoutException te) {
                log.error()
                        .exception(te)
                        .attr("request", request)
                        .log("Timeout to fence read entry");
            }
            sendResponse(data, BookieProtocol.EIO, startTimeNanos);
        }
    }

    @Override
    public String toString() {
        return String.format("ReadEntry(%d, %d)", request.getLedgerId(), request.getEntryId());
    }

    void recycle() {
        request.recycle();
        super.reset();
        if (this.recyclerHandle != null) {
            this.recyclerHandle.recycle(this);
        }
    }

    private final Recycler.Handle<ReadEntryProcessor> recyclerHandle;

    private ReadEntryProcessor(Recycler.Handle<ReadEntryProcessor> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    protected ReadEntryProcessor() {
        this.recyclerHandle = null;
    }

    private static final Recycler<ReadEntryProcessor> RECYCLER = new Recycler<ReadEntryProcessor>() {
        @Override
        protected ReadEntryProcessor newObject(Recycler.Handle<ReadEntryProcessor> handle) {
            return new ReadEntryProcessor(handle);
        }
    };
}
