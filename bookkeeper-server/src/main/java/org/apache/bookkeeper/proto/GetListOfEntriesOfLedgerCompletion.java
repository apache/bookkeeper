package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;

class GetListOfEntriesOfLedgerCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback cb;

    public GetListOfEntriesOfLedgerCompletion(final CompletionKey key,
                                              final BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback origCallback,
                                              final long ledgerId,
                                              PerChannelBookieClient perChannelBookieClient) {
        super("GetListOfEntriesOfLedger", null, ledgerId, 0L, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.getListOfEntriesOfLedgerCompletionTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.GetListOfEntriesOfLedgerCallback() {
            @Override
            public void getListOfEntriesOfLedgerComplete(int rc, long ledgerId,
                                                         AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger) {
                logOpResult(rc);
                origCallback.getListOfEntriesOfLedgerComplete(rc, ledgerId, availabilityOfEntriesOfLedger);
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
        errorOutAndRunCallback(() -> cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, null));
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.GetListOfEntriesOfLedgerResponse getListOfEntriesOfLedgerResponse = response
                .getGetListOfEntriesOfLedgerResponse();
        ByteBuf availabilityOfEntriesOfLedgerBuffer = Unpooled.EMPTY_BUFFER;
        BookkeeperProtocol.StatusCode status = response.getStatus() == BookkeeperProtocol.StatusCode.EOK ? getListOfEntriesOfLedgerResponse.getStatus()
                : response.getStatus();

        if (getListOfEntriesOfLedgerResponse.hasAvailabilityOfEntriesOfLedger()) {
            availabilityOfEntriesOfLedgerBuffer = Unpooled.wrappedBuffer(
                    getListOfEntriesOfLedgerResponse.getAvailabilityOfEntriesOfLedger().asReadOnlyByteBuffer());
        }

        if (LOG.isDebugEnabled()) {
            logResponse(status, "ledgerId", ledgerId);
        }

        int rc = convertStatus(status, BKException.Code.ReadException);
        AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = null;
        if (rc == BKException.Code.OK) {
            availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    availabilityOfEntriesOfLedgerBuffer.slice());
        }
        cb.getListOfEntriesOfLedgerComplete(rc, ledgerId, availabilityOfEntriesOfLedger);
    }
}
