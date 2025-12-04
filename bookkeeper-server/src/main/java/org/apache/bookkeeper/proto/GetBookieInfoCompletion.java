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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookieInfoReader;
import org.apache.bookkeeper.common.util.MathUtils;

class GetBookieInfoCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.GetBookieInfoCallback cb;

    public GetBookieInfoCompletion(final CompletionKey key,
                                   final BookkeeperInternalCallbacks.GetBookieInfoCallback origCallback,
                                   final Object origCtx,
                                   PerChannelBookieClient perChannelBookieClient) {
        super("GetBookieInfo", origCtx, 0L, 0L, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.getBookieInfoOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.getBookieInfoTimeoutOpLogger;
        this.cb = (rc, bInfo, ctx) -> {
            logOpResult(rc);
            origCallback.getBookieInfoComplete(rc, bInfo, origCtx);
            key.release();
        };
    }

    @Override
    boolean maybeTimeout() {
        if (MathUtils.elapsedNanos(startTime) >= perChannelBookieClient.getBookieInfoTimeoutNanos) {
            timeout();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(
                () -> cb.getBookieInfoComplete(rc, new BookieInfoReader.BookieInfo(), ctx));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.GetBookieInfoResponse getBookieInfoResponse = response.getGetBookieInfoResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? getBookieInfoResponse.getStatus() : response.getStatus();

        long freeDiskSpace = getBookieInfoResponse.getFreeDiskSpace();
        long totalDiskSpace = getBookieInfoResponse.getTotalDiskCapacity();

        if (LOG.isDebugEnabled()) {
            logResponse(status, "freeDisk", freeDiskSpace, "totalDisk", totalDiskSpace);
        }

        int rc = convertStatus(status, BKException.Code.ReadException);
        cb.getBookieInfoComplete(rc,
                new BookieInfoReader.BookieInfo(totalDiskSpace,
                        freeDiskSpace), ctx);
    }
}
