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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryProcessorV3 extends PacketProcessorBaseV3 {

    private static final Logger LOG = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    protected Stopwatch lastPhaseStartTime;
    private final ExecutorService fenceThreadPool;

    private SettableFuture<Boolean> fenceResult = null;

    protected final ReadRequest readRequest;
    protected final long ledgerId;
    protected final long entryId;

    // Stats
    protected final OpStatsLogger readStats;
    protected final OpStatsLogger reqStats;

    public ReadEntryProcessorV3(Request request,
                                Channel channel,
                                BookieRequestProcessor requestProcessor,
                                ExecutorService fenceThreadPool) {
        super(request, channel, requestProcessor);
        requestProcessor.onReadRequestStart(channel);

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
            Futures.addCallback(fenceResult, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    sendFenceResponse(readResponseBuilder, entryBody, result, startTimeSw);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.error("Fence request for ledgerId {} entryId {} encountered exception",
                            ledgerId, entryId, t);
                    sendFenceResponse(readResponseBuilder, entryBody, false, startTimeSw);
                }
            }, fenceThreadPool);
        } else {
            boolean success = false;
            try {
                success = fenceResult.get(1000, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                LOG.error("Fence request for ledgerId {} entryId {} encountered exception : ",
                        readRequest.getLedgerId(), readRequest.getEntryId(), t);
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
        throws IOException {
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
        throws IOException {
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

        final ReadResponse.Builder readResponse = ReadResponse.newBuilder()
            .setLedgerId(ledgerId)
            .setEntryId(entryId);
        try {
            // handle fence reqest
            if (RequestUtils.isFenceRequest(readRequest)) {
                LOG.info("Ledger fence request received for ledger: {} from address: {}", ledgerId,
                    channel.remoteAddress());
                if (!readRequest.hasMasterKey()) {
                    LOG.error(
                        "Fence ledger request received without master key for ledger:{} from address: {}",
                        ledgerId, channel.remoteAddress());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                } else {
                    byte[] masterKey = readRequest.getMasterKey().toByteArray();
                    fenceResult = requestProcessor.bookie.fenceLedger(ledgerId, masterKey);
                }
            }
            return readEntry(readResponse, entryId, startTimeSw);
        } catch (Bookie.NoLedgerException e) {
            if (RequestUtils.isFenceRequest(readRequest)) {
                LOG.info("No ledger found reading entry {} when fencing ledger {}", entryId, ledgerId);
            } else if (entryId != BookieProtocol.LAST_ADD_CONFIRMED) {
                LOG.info("No ledger found while reading entry: {} from ledger: {}", entryId, ledgerId);
            } else if (LOG.isDebugEnabled()) {
                // this is the case of a reader which is calling readLastAddConfirmed and the ledger is empty
                LOG.debug("No ledger found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
            return buildResponse(readResponse, StatusCode.ENOLEDGER, startTimeSw);
        } catch (Bookie.NoEntryException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No entry found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
            return buildResponse(readResponse, StatusCode.ENOENTRY, startTimeSw);
        } catch (IOException e) {
            LOG.error("IOException while reading entry: {} from ledger {} ", entryId, ledgerId, e);
            return buildResponse(readResponse, StatusCode.EIO, startTimeSw);
        } catch (BookieException e) {
            LOG.error(
                "Unauthorized access to ledger:{} while reading entry:{} in request from address: {}",
                    ledgerId, entryId, channel.remoteAddress());
            return buildResponse(readResponse, StatusCode.EUA, startTimeSw);
        }
    }

    @Override
    public void safeRun() {
        requestProcessor.getRequestStats().getReadEntrySchedulingDelayStats().registerSuccessfulEvent(
            MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);

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

