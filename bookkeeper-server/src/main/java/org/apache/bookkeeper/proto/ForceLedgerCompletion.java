package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.net.BookieId;

class ForceLedgerCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.ForceLedgerCallback cb;

    public ForceLedgerCompletion(final CompletionKey key,
                                 final BookkeeperInternalCallbacks.ForceLedgerCallback originalCallback,
                                 final Object originalCtx,
                                 final long ledgerId,
                                 PerChannelBookieClient perChannelBookieClient) {
        super("ForceLedger",
                originalCtx, ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.forceLedgerOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.forceLedgerTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.ForceLedgerCallback() {
            @Override
            public void forceLedgerComplete(int rc, long ledgerId,
                                            BookieId addr,
                                            Object ctx) {
                logOpResult(rc);
                originalCallback.forceLedgerComplete(rc, ledgerId,
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
                () -> cb.forceLedgerComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.ForceLedgerResponse forceLedgerResponse = response.getForceLedgerResponse();
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK
                ? forceLedgerResponse.getStatus() : response.getStatus();
        long ledgerId = forceLedgerResponse.getLedgerId();

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledger", ledgerId);
        }
        int rc = convertStatus(status, BKException.Code.WriteException);
        cb.forceLedgerComplete(rc, ledgerId, perChannelBookieClient.bookieId, ctx);
    }
}
