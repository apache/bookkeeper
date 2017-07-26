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

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor handling long poll read entry request.
 */
class LongPollReadEntryProcessorV3 extends ReadEntryProcessorV3 implements Observer {

    private final static Logger logger = LoggerFactory.getLogger(LongPollReadEntryProcessorV3.class);

    private final Long previousLAC;
    private Optional<Long> lastAddConfirmedUpdateTime = Optional.absent();

    // long poll execution state
    private final ExecutorService longPollThreadPool;
    private final HashedWheelTimer requestTimer;
    private Timeout expirationTimerTask = null;
    private Future<?> deferredTask = null;
    private boolean shouldReadEntry = false;

    LongPollReadEntryProcessorV3(Request request,
                                 Channel channel,
                                 BookieRequestProcessor requestProcessor,
                                 ExecutorService fenceThreadPool,
                                 ExecutorService longPollThreadPool,
                                 HashedWheelTimer requestTimer) {
        super(request, channel, requestProcessor, fenceThreadPool);
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
            throws IOException {
        if (RequestUtils.shouldPiggybackEntry(readRequest)) {
            if(!readRequest.hasPreviousLAC() || (BookieProtocol.LAST_ADD_CONFIRMED != entryId)) {
                // This is not a valid request - client bug?
                logger.error("Incorrect read request, entry piggyback requested incorrectly for ledgerId {} entryId {}",
                        ledgerId, entryId);
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
                    if (logger.isDebugEnabled()) {
                        logger.debug("ReadLAC Piggy Back reading entry:{} from ledger: {}", entryId, ledgerId);
                    }
                    try {
                        return super.readEntry(readResponseBuilder, entryId, true, startTimeSw);
                    } catch (Bookie.NoEntryException e) {
                        requestProcessor.readLastEntryNoEntryErrorCounter.inc();
                        logger.info("No entry found while piggyback reading entry {} from ledger {} : previous lac = {}",
                                new Object[] { entryId, ledgerId, previousLAC });
                        // piggy back is best effort and this request can fail genuinely because of striping
                        // entries across the ensemble
                        return buildResponse(readResponseBuilder, StatusCode.EOK, startTimeSw);
                    }
                } else {
                    if (knownLAC < previousLAC) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Found smaller lac when piggy back reading lac and entry from ledger {} :" +
                                    " previous lac = {}, known lac = {}",
                                    new Object[]{ ledgerId, previousLAC, knownLAC });
                        }
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
            if (logger.isTraceEnabled()) {
                logger.trace("Waiting For LAC Update {}", previousLAC);
            }

            final Stopwatch startTimeSw = Stopwatch.createStarted();

            final Observable observable;
            try {
                observable = requestProcessor.bookie.waitForLastAddConfirmedUpdate(ledgerId, previousLAC, this);
            } catch (Bookie.NoLedgerException e) {
                logger.info("No ledger found while longpoll reading ledger {}, previous lac = {}.",
                        ledgerId, previousLAC);
                return buildErrorResponse(StatusCode.ENOLEDGER, startTimeSw);
            } catch (IOException ioe) {
                logger.error("IOException while longpoll reading ledger {}, previous lac = {} : ",
                        new Object[] { ledgerId, previousLAC, ioe });
                return buildErrorResponse(StatusCode.EIO, startTimeSw);
            }

            registerSuccessfulEvent(requestProcessor.longPollPreWaitStats, startTimeSw);
            lastPhaseStartTime.reset().start();

            if (null != observable) {
                // successfully registered observable to lac updates
                if (logger.isTraceEnabled()) {
                    logger.trace("Waiting For LAC Update {}: Timeout {}", previousLAC, readRequest.getTimeOut());
                }
                synchronized (this) {
                    expirationTimerTask = requestTimer.newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            // When the timeout expires just get whatever is the current
                            // readLastConfirmed
                            LongPollReadEntryProcessorV3.this.scheduleDeferredRead(observable, true);
                        }
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
    public void update(Observable observable, Object o) {
        LastAddConfirmedUpdateNotification newLACNotification = (LastAddConfirmedUpdateNotification)o;
        if (newLACNotification.lastAddConfirmed > previousLAC) {
            if (newLACNotification.lastAddConfirmed != Long.MAX_VALUE &&
                    !lastAddConfirmedUpdateTime.isPresent()) {
                lastAddConfirmedUpdateTime = Optional.of(newLACNotification.timestamp);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Last Add Confirmed Advanced to {} for request {}",
                        newLACNotification.lastAddConfirmed, request);
            }
            scheduleDeferredRead(observable, false);
        }
    }

    private synchronized void scheduleDeferredRead(Observable observable, boolean timeout) {
        if (null == deferredTask) {
            if (logger.isTraceEnabled()) {
                logger.trace("Deferred Task, expired: {}, request: {}", timeout, request);
            }
            observable.deleteObserver(this);
            try {
                shouldReadEntry = true;
                deferredTask = longPollThreadPool.submit(this);
            } catch (RejectedExecutionException exc) {
                // If the threadPool has been shutdown, simply drop the task
            }
            if (null != expirationTimerTask) {
                expirationTimerTask.cancel();
            }

            registerEvent(timeout, requestProcessor.longPollWaitStats, lastPhaseStartTime);
            lastPhaseStartTime.reset().start();
        }
    }
}
