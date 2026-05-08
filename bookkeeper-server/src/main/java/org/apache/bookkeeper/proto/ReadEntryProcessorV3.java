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

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;

@CustomLog
class ReadEntryProcessorV3 extends PacketProcessorBaseV3 {

    protected Stopwatch lastPhaseStartTime;
    private final ExecutorService fenceThreadPool;

    private CompletableFuture<Boolean> fenceResult = null;

    protected final ReadRequest readRequest;
    protected final long ledgerId;
    protected final long entryId;

    // Stats
    protected final OpStatsLogger readStats;
    protected final OpStatsLogger reqStats;

    public ReadEntryProcessorV3(Request request,
                                BookieRequestHandler requestHandler,
                                BookieRequestProcessor requestProcessor,
                                ExecutorService fenceThreadPool) {
        super(request, requestHandler, requestProcessor);
        requestProcessor.onReadRequestStart(requestHandler.ctx().channel());

        this.readRequest = request.getReadRequest();
        this.ledgerId = readRequest.getLedgerId();
        this.entryId = readRequest.getEntryId();
        if (RequestUtils.isFenceRequest(this.readRequest)) {
            this.readStats = requestProcessor.getRequestStats().getFenceReadEntryStats();
            this.reqStats = requestProcessor.getRequestStats().getFenceReadRequestStats();
        } else if (readRequest.hasPreviousLAC()) {
            this.readStats = requestProcessor.getRequestStats().getLongPollReadStats();
            this.reqStats = requestProcessor.getRequestStats().getLongPollReadRequestStats();
        } else {
            this.readStats = requestProcessor.getRequestStats().getReadEntryStats();
            this.reqStats = requestProcessor.getRequestStats().getReadRequestStats();
        }

        this.fenceThreadPool = fenceThreadPool;
        lastPhaseStartTime = Stopwatch.createStarted();
    }

    protected Long getPreviousLAC() {
        if (readRequest.hasPreviousLAC()) {
            return readRequest.getPreviousLAC();
        } else {
            return null;
        }
    }

    /**
     * Handle read result for fence read.
     *
     * @param entryBody
     *          read result
     * @param readResponseBuilder
     *          read response builder
     * @param entryId
     *          entry id
     * @param startTimeSw
     *          timer for the read request
     */
    protected void handleReadResultForFenceRead(
        final ByteBuf entryBody,
        final ReadResponse.Builder readResponseBuilder,
        final long entryId,
        final Stopwatch startTimeSw) {
        // reset last phase start time to measure fence result waiting time
        lastPhaseStartTime.reset().start();
        if (null != fenceThreadPool) {
            fenceResult.whenCompleteAsync(new FutureEventListener<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    sendFenceResponse(readResponseBuilder, entryBody, result, startTimeSw);
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error()
                            .exception(t)
                            .attr("ledgerId", ledgerId)
                            .attr("entryId", entryId)
                            .log("Fence request encountered exception");
                    sendFenceResponse(readResponseBuilder, entryBody, false, startTimeSw);
                }
            }, fenceThreadPool);
        } else {
            boolean success = false;
            try {
                success = fenceResult.get(1000, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                log.error()
                        .exception(t)
                        .attr("ledgerId", readRequest.getLedgerId())
                        .attr("entryId", readRequest.getEntryId())
                        .log("Fence request encountered exception");
            }
            sendFenceResponse(readResponseBuilder, entryBody, success, startTimeSw);
        }
    }

    /**
     * Read a specific entry.
     *
     * @param readResponseBuilder
     *          read response builder.
     * @param entryId
     *          entry to read
     * @param startTimeSw
     *          stop watch to measure the read operation.
     * @return read response or null if it is a fence read operation.
     * @throws IOException
     */
    protected ReadResponse readEntry(ReadResponse.Builder readResponseBuilder,
                                     long entryId,
                                     Stopwatch startTimeSw)
        throws IOException, BookieException {
        return readEntry(readResponseBuilder, entryId, false, startTimeSw);
    }

    /**
     * Read a specific entry.
     *
     * @param readResponseBuilder
     *          read response builder.
     * @param entryId
     *          entry to read
     * @param startTimeSw
     *          stop watch to measure the read operation.
     * @return read response or null if it is a fence read operation.
     * @throws IOException
     */
    protected ReadResponse readEntry(ReadResponse.Builder readResponseBuilder,
                                     long entryId,
                                     boolean readLACPiggyBack,
                                     Stopwatch startTimeSw)
        throws IOException, BookieException {
        ByteBuf entryBody = requestProcessor.getBookie().readEntry(ledgerId, entryId);
        if (null != fenceResult) {
            handleReadResultForFenceRead(entryBody, readResponseBuilder, entryId, startTimeSw);
            return null;
        } else {
            try {
                readResponseBuilder.setBody(ByteString.copyFrom(entryBody.nioBuffer()));
                if (readLACPiggyBack) {
                    readResponseBuilder.setEntryId(entryId);
                } else {
                    long knownLAC = requestProcessor.getBookie().readLastAddConfirmed(ledgerId);
                    readResponseBuilder.setMaxLAC(knownLAC);
                }
                registerSuccessfulEvent(readStats, startTimeSw);
                readResponseBuilder.setStatus(StatusCode.EOK);
                return readResponseBuilder.build();
            } finally {
                ReferenceCountUtil.release(entryBody);
            }
        }
    }

    protected ReadResponse getReadResponse() {
        final Stopwatch startTimeSw = Stopwatch.createStarted();
        final Channel channel = requestHandler.ctx().channel();

        final ReadResponse.Builder readResponse = ReadResponse.newBuilder()
            .setLedgerId(ledgerId)
            .setEntryId(entryId);
        try {
            // handle fence request
            if (RequestUtils.isFenceRequest(readRequest)) {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("address", channel.remoteAddress())
                    .log("Ledger fence request received");
                if (!readRequest.hasMasterKey()) {
                    log.error()
                            .attr("ledgerId", ledgerId)
                            .attr("address", channel.remoteAddress())
                        .log("Fence ledger request received without master key");
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                } else {
                    byte[] masterKey = readRequest.getMasterKey().toByteArray();
                    fenceResult = requestProcessor.bookie.fenceLedger(ledgerId, masterKey);
                }
            }
            return readEntry(readResponse, entryId, startTimeSw);
        } catch (Bookie.NoLedgerException | BookieException.LedgerFencedAndDeletedException e) {
            if (RequestUtils.isFenceRequest(readRequest)) {
                log.info()
                        .attr("entryId", entryId)
                        .attr("ledgerId", ledgerId)
                    .log("No ledger found (or it has been deleted) reading entry when fencing ledger");
            } else if (entryId != BookieProtocol.LAST_ADD_CONFIRMED) {
                log.info()
                        .attr("entryId", entryId)
                        .attr("ledgerId", ledgerId)
                    .log("No ledger found while reading entry from ledger");
            } else {
                // this is the case of a reader which is calling readLastAddConfirmed and the ledger is empty
                log.debug()
                        .attr("entryId", entryId)
                        .attr("ledgerId", ledgerId)
                    .log("No ledger found while reading entry from ledger");
            }
            return buildResponse(readResponse, StatusCode.ENOLEDGER, startTimeSw);
        } catch (Bookie.NoEntryException e) {
            log.debug()
                    .attr("entryId", entryId)
                    .attr("ledgerId", ledgerId)
                    .log("No entry found while reading entry from ledger");
            return buildResponse(readResponse, StatusCode.ENOENTRY, startTimeSw);
        } catch (IOException e) {
            log.error()
                    .exception(e)
                    .attr("entryId", entryId)
                    .attr("ledgerId", ledgerId)
                    .log("IOException while reading entry from ledger");
            return buildResponse(readResponse, StatusCode.EIO, startTimeSw);
        } catch (BookieException.DataUnknownException e) {
            log.debug()
                    .attr("entryId", entryId)
                    .attr("ledgerId", ledgerId)
                    .log("Ledger has unknown state for entry");
            return buildResponse(readResponse, StatusCode.EUNKNOWNLEDGERSTATE, startTimeSw);
        } catch (BookieException e) {
            log.error()
                    .attr("ledgerId", ledgerId)
                    .attr("entryId", entryId)
                    .attr("address", channel.remoteAddress())
                    .log("Unauthorized access to ledger while reading entry");
            return buildResponse(readResponse, StatusCode.EUA, startTimeSw);
        }
    }

    @Override
    public void run() {
        requestProcessor.getRequestStats().getReadEntrySchedulingDelayStats().registerSuccessfulEvent(
            MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        if (!requestHandler.ctx().channel().isOpen()) {
            log.debug().attr("channel", () -> requestHandler.ctx().channel())
                    .log("Dropping read request for closed channel");
            requestProcessor.onReadRequestFinish();
            return;
        }

        if (!isVersionCompatible()) {
            ReadResponse readResponse = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setStatus(StatusCode.EBADVERSION)
                .build();
            sendResponse(readResponse);
            return;
        }

        executeOp();
    }

    protected void executeOp() {
        ReadResponse readResponse = getReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    private void getFenceResponse(ReadResponse.Builder readResponse,
                                  ByteBuf entryBody,
                                  boolean fenceResult) {
        StatusCode status;
        if (!fenceResult) {
            status = StatusCode.EIO;
            registerFailedEvent(requestProcessor.getRequestStats().getFenceReadWaitStats(), lastPhaseStartTime);
        } else {
            status = StatusCode.EOK;
            readResponse.setBody(ByteString.copyFrom(entryBody.nioBuffer()));
            registerSuccessfulEvent(requestProcessor.getRequestStats().getFenceReadWaitStats(), lastPhaseStartTime);
        }

        if (null != entryBody) {
            ReferenceCountUtil.release(entryBody);
        }

        readResponse.setStatus(status);
    }

    private void sendFenceResponse(ReadResponse.Builder readResponse,
                                   ByteBuf entryBody,
                                   boolean fenceResult,
                                   Stopwatch startTimeSw) {
        // build the fence read response
        getFenceResponse(readResponse, entryBody, fenceResult);
        // register fence read stat
        registerEvent(!fenceResult, requestProcessor.getRequestStats().getFenceReadEntryStats(), startTimeSw);
        // send the fence read response
        sendResponse(readResponse.build());
    }

    protected ReadResponse buildResponse(
            ReadResponse.Builder readResponseBuilder,
            StatusCode statusCode,
            Stopwatch startTimeSw) {
        registerEvent(!statusCode.equals(StatusCode.EOK), readStats, startTimeSw);
        readResponseBuilder.setStatus(statusCode);
        return readResponseBuilder.build();
    }

    protected void sendResponse(ReadResponse readResponse) {
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(readResponse.getStatus())
                .setReadResponse(readResponse);
        sendResponse(response.getStatus(),
                     response.build(),
                     reqStats);
        requestProcessor.onReadRequestFinish();
    }

    //
    // Stats Methods
    //

    protected void registerSuccessfulEvent(OpStatsLogger statsLogger, Stopwatch startTime) {
        registerEvent(false, statsLogger, startTime);
    }

    protected void registerFailedEvent(OpStatsLogger statsLogger, Stopwatch startTime) {
        registerEvent(true, statsLogger, startTime);
    }

    protected void registerEvent(boolean failed, OpStatsLogger statsLogger, Stopwatch startTime) {
        if (failed) {
            statsLogger.registerFailedEvent(startTime.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        } else {
            statsLogger.registerSuccessfulEvent(startTime.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * this toString method filters out masterKey from the output. masterKey
     * contains the password of the ledger.
     */
    @Override
    public String toString() {
        return RequestUtils.toSafeString(request);
    }
}

