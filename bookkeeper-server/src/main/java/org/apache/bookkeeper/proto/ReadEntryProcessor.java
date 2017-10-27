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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Recycler.Handle;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryProcessor extends PacketProcessorBase {
    private final static Logger LOG = LoggerFactory.getLogger(ReadEntryProcessor.class);

    public static ReadEntryProcessor create(Request request, Channel channel,
                              BookieRequestProcessor requestProcessor) {
        ReadEntryProcessor rep = RECYCLER.get();
        rep.init(request, channel, requestProcessor);
        return rep;
    }

    @Override
    protected void processPacket() {
        assert (request instanceof BookieProtocol.ReadRequest);
        BookieProtocol.ReadRequest read = (BookieProtocol.ReadRequest) request;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received new read request: {}", request);
        }
        int errorCode = BookieProtocol.EIO;
        long startTimeNanos = MathUtils.nowInNano();
        ByteBuf data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.isFencingRequest()) {
                LOG.warn("Ledger: {}  fenced by: {}", request.getLedgerId(), channel.remoteAddress());

                if (read.hasMasterKey()) {
                    fenceResult = requestProcessor.bookie.fenceLedger(read.getLedgerId(), read.getMasterKey());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = requestProcessor.bookie.readEntry(request.getLedgerId(), request.getEntryId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("##### Read entry ##### {} -- ref-count: {}", data.readableBytes(), data.refCnt());
            }
            if (null != fenceResult) {
                // TODO: {@link https://github.com/apache/bookkeeper/issues/283}
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
                        data.release();
                        data = null;
                    } else {
                        errorCode = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = BookieProtocol.EIO;
                    data.release();
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = BookieProtocol.EIO;
                    data.release();
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = BookieProtocol.EIO;
                    data.release();
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

        if (LOG.isTraceEnabled()) {
            LOG.trace("Read entry rc = {} for {}", new Object[] { errorCode, read });
        }
        if (errorCode == BookieProtocol.EOK) {
            requestProcessor.readEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(errorCode, ResponseBuilder.buildReadResponse(data, read),
                         requestProcessor.readRequestStats);

        } else {
            ReferenceCountUtil.release(data);

            requestProcessor.readEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            sendResponse(errorCode, ResponseBuilder.buildErrorResponse(errorCode, read),
                         requestProcessor.readRequestStats);
        }
        recycle();
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
