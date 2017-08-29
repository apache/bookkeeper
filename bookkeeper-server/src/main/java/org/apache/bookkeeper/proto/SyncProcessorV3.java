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

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
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

        StatusCode status = null;
        byte[] masterKey = syncRequest.getMasterKey().toByteArray();

        // TODO: code the 'sync'
        status = StatusCode.EOK;
        syncResponse.setLastPersistedEntryId(lastEntryId);

        // If everything is okay, we return null so that the calling function
        // dosn't return a response back to the caller.
        if (status.equals(StatusCode.EOK)) {
            requestProcessor.syncStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        } else {
            requestProcessor.syncStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
        }
        syncResponse.setStatus(status);
        return syncResponse.build();
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
