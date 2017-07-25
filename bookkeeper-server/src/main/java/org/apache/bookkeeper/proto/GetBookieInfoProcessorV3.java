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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.GetBookieInfoResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

public class GetBookieInfoProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(GetBookieInfoProcessorV3.class);

    public GetBookieInfoProcessorV3(Request request, Channel channel,
                                     BookieRequestProcessor requestProcessor) {
        super(request, channel, requestProcessor);
    }

    private GetBookieInfoResponse getGetBookieInfoResponse() {
        long startTimeNanos = MathUtils.nowInNano();
        GetBookieInfoRequest getBookieInfoRequest = request.getGetBookieInfoRequest();
        long requested = getBookieInfoRequest.getRequested();

        GetBookieInfoResponse.Builder getBookieInfoResponse = GetBookieInfoResponse.newBuilder();

        if (!isVersionCompatible()) {
            getBookieInfoResponse.setStatus(StatusCode.EBADVERSION);
            requestProcessor.getBookieInfoStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                    TimeUnit.NANOSECONDS);
            return getBookieInfoResponse.build();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received new getBookieInfo request: {}", request);
        }
        StatusCode status = StatusCode.EOK;
        long freeDiskSpace = 0L, totalDiskSpace = 0L;
        try {
            if ((requested & GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                freeDiskSpace = requestProcessor.bookie.getTotalFreeSpace();
                getBookieInfoResponse.setFreeDiskSpace(freeDiskSpace);
            }
            if ((requested & GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
                totalDiskSpace = requestProcessor.bookie.getTotalDiskSpace();
                getBookieInfoResponse.setTotalDiskCapacity(totalDiskSpace);
            }
            LOG.debug("FreeDiskSpace info is " + freeDiskSpace + " totalDiskSpace is: " + totalDiskSpace);
        } catch (IOException e) {
            status = StatusCode.EIO;
            LOG.error("IOException while getting  freespace/totalspace", e);
        }

        getBookieInfoResponse.setStatus(status);
        requestProcessor.getBookieInfoStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                TimeUnit.NANOSECONDS);
        return getBookieInfoResponse.build();
    }

    @Override
    public void safeRun() {
        GetBookieInfoResponse getBookieInfoResponse = getGetBookieInfoResponse();
        sendResponse(getBookieInfoResponse);
    }

    private void sendResponse(GetBookieInfoResponse getBookieInfoResponse) {
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(getBookieInfoResponse.getStatus())
                .setGetBookieInfoResponse(getBookieInfoResponse);
        sendResponse(response.getStatus(),
                     response.build(),
                     requestProcessor.getBookieInfoRequestStats);
    }
}
