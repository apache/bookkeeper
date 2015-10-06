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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes add entry requests
 */
class WriteEntryProcessor extends PacketProcessorBase implements WriteCallback {

    private final static Logger LOG = LoggerFactory.getLogger(WriteEntryProcessor.class);

    long startTimeNanos;

    public WriteEntryProcessor(Request request, Channel channel,
                               BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
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
            return;
        }

        startTimeNanos = MathUtils.nowInNano();
        int rc = BookieProtocol.EOK;
        try {
            if (add.isRecoveryAdd()) {
                requestProcessor.bookie.recoveryAddEntry(add.getDataAsByteBuffer(),
                                                         this, channel, add.getMasterKey());
            } else {
                requestProcessor.bookie.addEntry(add.getDataAsByteBuffer(),
                                                 this, channel, add.getMasterKey());
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
        }
        if (rc != BookieProtocol.EOK) {
            requestProcessor.addEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(rc,
                         ResponseBuilder.buildErrorResponse(rc, add),
                         requestProcessor.addRequestStats);
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
    }

    @Override
    public String toString() {
        return String.format("WriteEntry(%d, %d)",
                             request.getLedgerId(), request.getEntryId());
    }
}
