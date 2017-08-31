/**
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import java.io.IOException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.SyncRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.SyncResponse;

class SyncProcessorV3 extends PacketProcessorBaseV3 implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(SyncProcessorV3.class);

    public SyncProcessorV3(Request request, Channel channel,
        BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    // Returns null if there is no exception thrown
    private SyncResponse getSyncResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        SyncRequest syncRequest = request.getSyncRequest();
        long firstEntryId = syncRequest.getFirstEntryId();
        long lastEntryId = syncRequest.getLastEntryId();
        long ledgerId = syncRequest.getLedgerId();

        final SyncResponse.Builder syncResponse = SyncResponse.newBuilder().setLedgerId(ledgerId);

        if (!isVersionCompatible()) {
            syncResponse.setStatus(StatusCode.EBADVERSION);
            return syncResponse.build();
        }

        if (requestProcessor.bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode, so rejecting the request from the client!");
            syncResponse.setStatus(StatusCode.EREADONLY);
            return syncResponse.build();
        }
        
        byte[] masterKey = syncRequest.getMasterKey().toByteArray();

        BookkeeperInternalCallbacks.SyncCallback wcb = new BookkeeperInternalCallbacks.SyncCallback() {
            @Override
            public void syncComplete(int rc, long ledgerId, long lastSyncedEntryId, BookieSocketAddress addr, Object ctx) {
                logger.info("sync ledg "+ledgerId+", lastAddSyncedEntry:"+lastSyncedEntryId);
                if (BookieProtocol.EOK == rc) {
                    requestProcessor.syncStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                } else {
                    requestProcessor.syncStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                            TimeUnit.NANOSECONDS);
                }

                StatusCode status;
                switch (rc) {
                    case BookieProtocol.EOK:
                        status = StatusCode.EOK;
                        break;
                    case BookieProtocol.EIO:
                        status = StatusCode.EIO;
                        break;
                    default:
                        status = StatusCode.EUA;
                        break;
                }
                syncResponse.setStatus(status);
                syncResponse.setLastPersistedEntryId(lastSyncedEntryId);
                
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(syncResponse.getStatus())
                        .setSyncResponse(syncResponse);
                Response resp = response.build();
                sendResponse(status, resp, requestProcessor.syncRequestStats);
            }
        };
        
        StatusCode status;
        try {            
            requestProcessor.bookie.sync(ledgerId, firstEntryId, lastEntryId, wcb, channel, masterKey);
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error syncing to entry:{} to ledger:{}",
                         new Object[] { lastEntryId, ledgerId, e });
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.debug("Ledger fenced while syncing to entry:{} to ledger:{}",
                         lastEntryId, ledgerId);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while syncing to entry:{}",
                         ledgerId, lastEntryId);
            status = StatusCode.EUA;
        } catch (Throwable t) {
            logger.error("Unexpected exception while syncing to {}@{} : ",
                         new Object[] { lastEntryId, ledgerId, t });
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }
        
       // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            syncResponse.setStatus(status);
            return syncResponse.build();
        }
        return null;
    }

    @Override
    public void safeRun() {
        SyncResponse syncResponse = getSyncResponse();
        if (null != syncResponse) {
            Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(syncResponse.getStatus())
                .setSyncResponse(syncResponse);
            Response resp = response.build();
            sendResponse(syncResponse.getStatus(), resp, requestProcessor.syncRequestStats);
        }
    }
}
