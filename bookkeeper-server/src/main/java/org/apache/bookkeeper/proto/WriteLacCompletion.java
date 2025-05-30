package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.net.BookieId;

class WriteLacCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.WriteLacCallback cb;

    public WriteLacCompletion(final CompletionKey key,
                              final BookkeeperInternalCallbacks.WriteLacCallback originalCallback,
                              final Object originalCtx,
                              final long ledgerId,
                              PerChannelBookieClient perChannelBookieClient) {
        super("WriteLAC",
                originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.writeLacOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.writeLacTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.WriteLacCallback() {
            @Override
            public void writeLacComplete(int rc, long ledgerId,
                                         BookieId addr,
                                         Object ctx) {
                logOpResult(rc);
                originalCallback.writeLacComplete(rc, ledgerId,
                        addr, originalCtx);
                key.release();
            }
        };
    }

    @Override
    public void errorOut() {
        errorOut(BKException.Code.BookieHandleNotAvailableException);
    }

    @Override
    public void errorOut(final int rc) {
        errorOutAndRunCallback(
                () -> cb.writeLacComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.WriteLacResponse writeLacResponse = response.getWriteLacResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? writeLacResponse.getStatus() : response.getStatus();
        long ledgerId = writeLacResponse.getLedgerId();

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledger", ledgerId);
        }
        int rc = convertStatus(status, BKException.Code.WriteException);
        cb.writeLacComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx);
    }
}
