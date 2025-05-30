package org.apache.bookkeeper.proto;

class TxnCompletionKey extends CompletionKey {
    final long txnId;

    public TxnCompletionKey(long txnId, BookkeeperProtocol.OperationType operationType) {
        super(operationType);
        this.txnId = txnId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TxnCompletionKey)) {
            return false;
        }
        TxnCompletionKey that = (TxnCompletionKey) obj;
        return this.txnId == that.txnId && this.operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        return ((int) txnId);
    }

    @Override
    public String toString() {
        return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
    }

}
