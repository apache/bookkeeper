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
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryProcessor extends PacketProcessorBase {
    private final static Logger LOG = LoggerFactory.getLogger(ReadEntryProcessor.class);

    public ReadEntryProcessor(Request request, Channel channel,
                              BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    @Override
    protected void processPacket() {
        assert (request instanceof BookieProtocol.ReadRequest);
        BookieProtocol.ReadRequest read = (BookieProtocol.ReadRequest) request;

        LOG.debug("Received new read request: {}", request);
        int errorCode = BookieProtocol.EIO;
        long startTimeNanos = MathUtils.nowInNano();
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.isFencingRequest()) {
                LOG.warn("Ledger " + request.getLedgerId() + " fenced by " + channel.getRemoteAddress());

                if (read.hasMasterKey()) {
                    fenceResult = requestProcessor.bookie.fenceLedger(read.getLedgerId(), read.getMasterKey());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = requestProcessor.bookie.readEntry(request.getLedgerId(), request.getEntryId());
            LOG.debug("##### Read entry ##### {}", data.remaining());
            if (null != fenceResult) {
                // TODO:
                // currently we don't have readCallback to run in separated read
                // threads. after BOOKKEEPER-429 is complete, we could improve
                // following code to make it not wait here
                //
                // For now, since we only try to wait after read entry. so writing
                // to journal and read entry are executed in different thread
                // it would be fine.
                try {
                    Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                    if (null == fenced || !fenced) {
                        // if failed to fence, fail the read request to make it retry.
                        errorCode = BookieProtocol.EIO;
                        data = null;
                    } else {
                        errorCode = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                }
            } else {
                errorCode = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + read.getLedgerId(), e);
            errorCode = BookieProtocol.EUA;
        }

        LOG.trace("Read entry rc = {} for {}",
                new Object[] { errorCode, read });
        if (errorCode == BookieProtocol.EOK) {
            requestProcessor.readEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(errorCode, ResponseBuilder.buildReadResponse(data, read),
                         requestProcessor.readRequestStats);

        } else {
            requestProcessor.readEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(errorCode, ResponseBuilder.buildErrorResponse(errorCode, read),
                         requestProcessor.readRequestStats);
        }
    }

    @Override
    public String toString() {
        return String.format("ReadEntry(%d, %d)", request.getLedgerId(), request.getEntryId());
    }
}
