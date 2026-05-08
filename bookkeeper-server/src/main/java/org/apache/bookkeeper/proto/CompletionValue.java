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

import io.github.merlimat.slog.Event;
import io.netty.channel.Channel;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.MdcUtils;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.MDC;

@CustomLog
abstract class CompletionValue {
    private final String operationName;
    protected Object ctx;
    protected long ledgerId;
    protected long entryId;
    protected long startTime;
    protected OpStatsLogger opLogger;
    protected OpStatsLogger timeoutOpLogger;
    protected Map<String, String> mdcContextMap;
    protected PerChannelBookieClient perChannelBookieClient;

    public CompletionValue(String operationName,
                           Object ctx,
                           long ledgerId, long entryId, PerChannelBookieClient perChannelBookieClient) {
        this.operationName = operationName;
        this.ctx = ctx;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.startTime = MathUtils.nowInNano();
        this.perChannelBookieClient = perChannelBookieClient;
        if (perChannelBookieClient != null) {
            this.mdcContextMap = perChannelBookieClient.preserveMdcForTaskExecution ? MDC.getCopyOfContextMap() : null;
        }
    }

    private long latency() {
        return MathUtils.elapsedNanos(startTime);
    }

    void logOpResult(int rc) {
        if (rc != BKException.Code.OK) {
            opLogger.registerFailedEvent(latency(), TimeUnit.NANOSECONDS);
        } else {
            opLogger.registerSuccessfulEvent(latency(), TimeUnit.NANOSECONDS);
        }

        if (rc != BKException.Code.OK
                && !PerChannelBookieClient.EXPECTED_BK_OPERATION_ERRORS.contains(rc)) {
            perChannelBookieClient.recordError();
        }
    }

    boolean maybeTimeout() {
        if (MathUtils.elapsedNanos(startTime) >= perChannelBookieClient.readEntryTimeoutNanos) {
            timeout();
            return true;
        } else {
            return false;
        }
    }

    void timeout() {
        timeoutOpLogger.registerSuccessfulEvent(latency(),
                TimeUnit.NANOSECONDS);
        errorOut(BKException.Code.TimeoutException);
    }
    protected Event logEvent(BookkeeperProtocol.StatusCode status) {
        return log.debug()
                .attr("operation", operationName)
                .attr("bookieId", perChannelBookieClient.bookieId)
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .attr("status", status);
    }

    protected int convertStatus(BookkeeperProtocol.StatusCode status, int defaultStatus) {
        // convert to BKException code
        int rcToRet = statusCodeToExceptionCode(status);
        if (rcToRet == BKException.Code.UNINITIALIZED) {
            log.error()
                    .attr("operation", operationName)
                    .attr("bookieId", perChannelBookieClient.bookieId)
                    .attr("ledgerId", ledgerId)
                    .attr("entryId", entryId)
                    .attr("statusCode", status)
                    .log("Operation failed on bookie");
            return defaultStatus;
        } else {
            return rcToRet;
        }
    }

    /**
     * @param status
     * @return {@link BKException.Code.UNINITIALIZED} if the statuscode is unknown.
     */
    private int statusCodeToExceptionCode(BookkeeperProtocol.StatusCode status) {
        switch (status) {
            case EOK:
                return BKException.Code.OK;
            case ENOENTRY:
                return BKException.Code.NoSuchEntryException;
            case ENOLEDGER:
                return BKException.Code.NoSuchLedgerExistsException;
            case EBADVERSION:
                return BKException.Code.ProtocolVersionException;
            case EUA:
                return BKException.Code.UnauthorizedAccessException;
            case EFENCED:
                return BKException.Code.LedgerFencedException;
            case EREADONLY:
                return BKException.Code.WriteOnReadOnlyBookieException;
            case ETOOMANYREQUESTS:
                return BKException.Code.TooManyRequestsException;
            case EUNKNOWNLEDGERSTATE:
                return BKException.Code.DataUnknownException;
            default:
                return BKException.Code.UNINITIALIZED;
        }
    }

    public void restoreMdcContext() {
        MdcUtils.restoreContext(mdcContextMap);
    }

    public abstract void errorOut();
    public abstract void errorOut(int rc);
    public void setOutstanding() {
        // no-op
    }

    protected void errorOutAndRunCallback(final Runnable callback) {
        perChannelBookieClient.executor.executeOrdered(ledgerId, () -> {
            String bAddress = "null";
            Channel c = perChannelBookieClient.channel;
            if (c != null && c.remoteAddress() != null) {
                bAddress = c.remoteAddress().toString();
            }
            log.debug()
                    .attr("operation", operationName)
                    .attr("bookieAddress", bAddress)
                    .attr("ledgerId", ledgerId).attr("entryId", entryId)
                    .log("Could not write request to bookie");
            callback.run();
        });
    }

    public void handleV2Response(
            long ledgerId, long entryId, BookkeeperProtocol.StatusCode status,
            BookieProtocol.Response response) {
        log.warn().attr("response", response).log("Unhandled V2 response");
    }

    public abstract void handleV3Response(
            BookkeeperProtocol.Response response);

    public void release() {}
}
