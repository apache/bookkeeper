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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookieProtocol.ReadRequest;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ReadEntryProcessor extends PacketProcessorBase<ReadRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadEntryProcessor.class);

    private ExecutorService fenceThreadPool;

    public static ReadEntryProcessor create(ReadRequest request,
                                            Channel channel,
                                            BookieRequestProcessor requestProcessor,
                                            ExecutorService fenceThreadPool) {
        ReadEntryProcessor rep = RECYCLER.get();
        rep.init(request, channel, requestProcessor);
        rep.fenceThreadPool = fenceThreadPool;
        return rep;
    }

    @Override
    protected void processPacket() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received new read request: {}", request);
        }
        int errorCode = BookieProtocol.EOK;
        long startTimeNanos = MathUtils.nowInNano();
        ByteBuf data = null;
        try {
            SettableFuture<Boolean> fenceResult = null;
            if (request.isFencing()) {
                LOG.warn("Ledger: {}  fenced by: {}", request.getLedgerId(), channel.remoteAddress());

                if (request.hasMasterKey()) {
                    fenceResult = requestProcessor.getBookie().fenceLedger(request.getLedgerId(),
                            request.getMasterKey());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", request.getLedgerId());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = requestProcessor.getBookie().readEntry(request.getLedgerId(), request.getEntryId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("##### Read entry ##### {} -- ref-count: {}", data.readableBytes(), data.refCnt());
            }
            if (fenceResult != null) {
                handleReadResultForFenceRead(fenceResult, data, startTimeNanos);
                return;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error reading {}", request, e);
            }
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error reading {}", request, e);
            }
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error reading {}", request, e);
            }
            errorCode = BookieProtocol.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger {}", request.getLedgerId(), e);
            errorCode = BookieProtocol.EUA;
        } catch (Throwable t) {
            LOG.error("Unexpected exception reading at {}:{} : {}", request.getLedgerId(), request.getEntryId(),
                      t.getMessage(), t);
            errorCode = BookieProtocol.EBADREQ;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Read entry rc = {} for {}", errorCode, request);
        }
        sendResponse(data, errorCode, startTimeNanos);
    }

    private void sendResponse(ByteBuf data, int errorCode, long startTimeNanos) {
        final RequestStats stats = requestProcessor.getRequestStats();
        final OpStatsLogger logger = stats.getReadEntryStats();
        BookieProtocol.Response response;
        if (errorCode == BookieProtocol.EOK) {
            logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            response = ResponseBuilder.buildReadResponse(data, request);
        } else {
            if (data != null) {
                ReferenceCountUtil.release(data);
            }
            logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            response = ResponseBuilder.buildErrorResponse(errorCode, request);
        }
        sendResponse(errorCode, response, stats.getReadRequestStats());
        recycle();
    }

    private void sendFenceResponse(Boolean result, ByteBuf data, long startTimeNanos) {
        final int retCode = result != null && result ? BookieProtocol.EOK : BookieProtocol.EIO;
        sendResponse(data, retCode, startTimeNanos);
    }

    private void handleReadResultForFenceRead(ListenableFuture<Boolean> fenceResult,
                                              ByteBuf data,
                                              long startTimeNanos) {
        if (null != fenceThreadPool) {
            Futures.addCallback(fenceResult, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    sendFenceResponse(result, data, startTimeNanos);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.error("Error processing fence request", t);
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
                LOG.error("Interrupting fence read entry {}", request, ie);
            } catch (ExecutionException ee) {
                LOG.error("Failed to fence read entry {}", request, ee.getCause());
            } catch (TimeoutException te) {
                LOG.error("Timeout to fence read entry {}", request, te);
            }
            sendResponse(data, BookieProtocol.EIO, startTimeNanos);
        }
    }

    @Override
    public String toString() {
        return String.format("ReadEntry(%d, %d)", request.getLedgerId(), request.getEntryId());
    }

    private void recycle() {
        super.reset();
        this.recyclerHandle.recycle(this);
    }

    private final Recycler.Handle<ReadEntryProcessor> recyclerHandle;

    private ReadEntryProcessor(Recycler.Handle<ReadEntryProcessor> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<ReadEntryProcessor> RECYCLER = new Recycler<ReadEntryProcessor>() {
        @Override
        protected ReadEntryProcessor newObject(Recycler.Handle<ReadEntryProcessor> handle) {
            return new ReadEntryProcessor(handle);
        }
    };
}
