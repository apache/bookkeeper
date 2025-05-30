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
        this.cb = new BookkeeperInternalCallbacks.GetBookieInfoCallback() {
            @Override
            public void getBookieInfoComplete(int rc, BookieInfoReader.BookieInfo bInfo,
                                              Object ctx) {
                logOpResult(rc);
                origCallback.getBookieInfoComplete(rc, bInfo, origCtx);
                key.release();
            }
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
