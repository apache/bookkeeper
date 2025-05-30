package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.BKException;

class StartTLSCompletion extends CompletionValue {
    final BookkeeperInternalCallbacks.StartTLSCallback cb;

    public StartTLSCompletion(final CompletionKey key, PerChannelBookieClient perChannelBookieClient) {
        super("StartTLS", null, -1, -1, perChannelBookieClient);
        this.opLogger = perChannelBookieClient.startTLSOpLogger;
        this.timeoutOpLogger = perChannelBookieClient.startTLSTimeoutOpLogger;
        this.cb = new BookkeeperInternalCallbacks.StartTLSCallback() {
            @Override
            public void startTLSComplete(int rc, Object ctx) {
                logOpResult(rc);
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
        perChannelBookieClient.failTLS(rc);
    }

    @Override
    public void handleV3Response(BookkeeperProtocol.Response response) {
        BookkeeperProtocol.StatusCode status = response.getStatus();

        if (LOG.isDebugEnabled()) {
            logResponse(status);
        }

        int rc = convertStatus(status, BKException.Code.SecurityException);

        // Cancel START_TLS request timeout
        cb.startTLSComplete(rc, null);

        if (perChannelBookieClient.state != PerChannelBookieClient.ConnectionState.START_TLS) {
            LOG.error("Connection state changed before TLS response received");
            perChannelBookieClient.failTLS(BKException.Code.BookieHandleNotAvailableException);
        } else if (status != BookkeeperProtocol.StatusCode.EOK) {
            LOG.error("Client received error {} during TLS negotiation", status);
            perChannelBookieClient.failTLS(BKException.Code.SecurityException);
        } else {
            perChannelBookieClient.initTLSHandshake();
        }
    }
}
