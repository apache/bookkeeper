package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;

abstract class CompletionKey {
    OperationType operationType;

    CompletionKey(OperationType operationType) {
        this.operationType = operationType;
    }

    public void release() {}
}
