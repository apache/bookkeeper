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
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

/**
 * Processor handling long poll read entry request.
 */
@CustomLog
class LongPollReadEntryProcessorV3 extends ReadEntryProcessorV3 implements Watcher<LastAddConfirmedUpdateNotification> {

    private final Long previousLAC;
    private Optional<Long> lastAddConfirmedUpdateTime = Optional.empty();

    // long poll execution state
    private final ExecutorService longPollThreadPool;
    private final HashedWheelTimer requestTimer;
    private Timeout expirationTimerTask = null;
    private Future<?> deferredTask = null;
    private boolean shouldReadEntry = false;

    LongPollReadEntryProcessorV3(Request request,
                                 BookieRequestHandler requestHandler,
                                 BookieRequestProcessor requestProcessor,
                                 ExecutorService fenceThreadPool,
                                 ExecutorService longPollThreadPool,
                                 HashedWheelTimer requestTimer) {
        super(request, requestHandler, requestProcessor, fenceThreadPool);
        this.previousLAC = readRequest.getPreviousLAC();
        this.longPollThreadPool = longPollThreadPool;
        this.requestTimer = requestTimer;

    }

    @Override
    protected Long getPreviousLAC() {
        return previousLAC;
    }

    private synchronized boolean shouldReadEntry() {
        return shouldReadEntry;
    }

    @Override
    protected ReadResponse readEntry(ReadResponse.Builder readResponseBuilder,
                                     long entryId,
                                     Stopwatch startTimeSw)
            throws IOException, BookieException {
        if (RequestUtils.shouldPiggybackEntry(readRequest)) {
            if (!readRequest.hasPreviousLAC() || (BookieProtocol.LAST_ADD_CONFIRMED != entryId)) {
                // This is not a valid request - client bug?
                log.error()
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", entryId)
                        .log("Incorrect read request, entry piggyback requested incorrectly");
                return buildResponse(readResponseBuilder, StatusCode.EBADREQ, startTimeSw);
            } else {
                long knownLAC = requestProcessor.bookie.readLastAddConfirmed(ledgerId);
                readResponseBuilder.setMaxLAC(knownLAC);
                if (knownLAC > previousLAC) {
                    entryId = previousLAC + 1;
                    readResponseBuilder.setMaxLAC(knownLAC);
                    if (lastAddConfirmedUpdateTime.isPresent()) {
                        readResponseBuilder.setLacUpdateTimestamp(lastAddConfirmedUpdateTime.get());
                    }
                    log.debug()
                            .attr("entryId", entryId)
                            .attr("ledgerId", ledgerId)
                            .log("ReadLAC Piggy Back reading entry");
                    try {
                        return super.readEntry(readResponseBuilder, entryId, true, startTimeSw);
                    } catch (Bookie.NoEntryException e) {
                        requestProcessor.getRequestStats().getReadLastEntryNoEntryErrorCounter().inc();
                        log.info()
                                .attr("entryId", entryId)
                                .attr("ledgerId", ledgerId)
                                .attr("previousLAC", previousLAC)
                                .log("No entry found while piggyback reading entry");
                        // piggy back is best effort and this request can fail genuinely because of striping
                        // entries across the ensemble
                        return buildResponse(readResponseBuilder, StatusCode.EOK, startTimeSw);
                    }
                } else {
                    if (knownLAC < previousLAC) {
                        log.debug()
                                .attr("ledgerId", ledgerId)
                                .attr("previousLAC", previousLAC)
                                .attr("knownLAC", knownLAC)
                                .log("Found smaller lac when piggy back reading lac and entry");
                    }
                    return buildResponse(readResponseBuilder, StatusCode.EOK, startTimeSw);
                }
            }
        } else {
            return super.readEntry(readResponseBuilder, entryId, false, startTimeSw);
        }
    }

    private ReadResponse buildErrorResponse(StatusCode statusCode, Stopwatch sw) {
        ReadResponse.Builder builder = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        return buildResponse(builder, statusCode, sw);
    }

    private ReadResponse getLongPollReadResponse() {
        if (!shouldReadEntry() && readRequest.hasTimeOut()) {
            log.trace().attr("previousLAC", previousLAC).log("Waiting For LAC Update");

            final Stopwatch startTimeSw = Stopwatch.createStarted();

            final boolean watched;
            try {
                watched = requestProcessor.getBookie().waitForLastAddConfirmedUpdate(ledgerId, previousLAC, this);
            } catch (Bookie.NoLedgerException e) {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("previousLAC", previousLAC)
                        .log("No ledger found while longpoll reading");
                return buildErrorResponse(StatusCode.ENOLEDGER, startTimeSw);
            } catch (IOException ioe) {
                log.error()
                        .exception(ioe)
                        .attr("ledgerId", ledgerId)
                        .attr("previousLAC", previousLAC)
                        .log("IOException while longpoll reading");
                return buildErrorResponse(StatusCode.EIO, startTimeSw);
            }

            registerSuccessfulEvent(requestProcessor.getRequestStats().getLongPollPreWaitStats(), startTimeSw);
            lastPhaseStartTime.reset().start();

            if (watched) {
                // successfully registered watcher to lac updates
                log.trace()
                        .attr("previousLAC", previousLAC)
                        .attr("timeout", readRequest.getTimeOut())
                        .log("Waiting For LAC Update");
                synchronized (this) {
                    expirationTimerTask = requestTimer.newTimeout(timeout -> {
                            requestProcessor.getBookie().cancelWaitForLastAddConfirmedUpdate(ledgerId, this);
                            // When the timeout expires just get whatever is the current
                            // readLastConfirmed
                            LongPollReadEntryProcessorV3.this.scheduleDeferredRead(true);
                    }, readRequest.getTimeOut(), TimeUnit.MILLISECONDS);
                }
                return null;
            }
        }
        // request doesn't have timeout or fail to wait, proceed to read entry
        return getReadResponse();
    }

    @Override
    protected void executeOp() {
        ReadResponse readResponse = getLongPollReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    @Override
    public void update(LastAddConfirmedUpdateNotification newLACNotification) {
        if (newLACNotification.getLastAddConfirmed() > previousLAC) {
            if (newLACNotification.getLastAddConfirmed() != Long.MAX_VALUE && !lastAddConfirmedUpdateTime.isPresent()) {
                lastAddConfirmedUpdateTime = Optional.of(newLACNotification.getTimestamp());
            }
            log.trace().attr("lastAddConfirmed", newLACNotification.getLastAddConfirmed())
                    .attr("request", request).log("Last Add Confirmed Advanced");
            scheduleDeferredRead(false);
        }
        newLACNotification.recycle();
    }

    private synchronized void scheduleDeferredRead(boolean timeout) {
        if (null == deferredTask) {
            log.trace()
                    .attr("expired", timeout)
                    .attr("request", request)
                    .log("Deferred Task");
            try {
                shouldReadEntry = true;
                deferredTask = longPollThreadPool.submit(this);
            } catch (RejectedExecutionException exc) {
                // If the threadPool has been shutdown, simply drop the task
            }
            if (null != expirationTimerTask) {
                expirationTimerTask.cancel();
            }

            registerEvent(timeout, requestProcessor.getRequestStats().getLongPollWaitStats(), lastPhaseStartTime);
            lastPhaseStartTime.reset().start();
        }
    }
}
