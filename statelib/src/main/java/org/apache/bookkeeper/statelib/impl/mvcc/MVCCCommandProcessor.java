/*
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
 */
package org.apache.bookkeeper.statelib.impl.mvcc;

import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.statelib.api.mvcc.op.DeleteOp;
import org.apache.bookkeeper.statelib.api.mvcc.op.IncrementOp;
import org.apache.bookkeeper.statelib.api.mvcc.op.PutOp;
import org.apache.bookkeeper.statelib.api.mvcc.result.Code;
import org.apache.bookkeeper.statelib.api.mvcc.result.DeleteResult;
import org.apache.bookkeeper.statelib.api.mvcc.result.IncrementResult;
import org.apache.bookkeeper.statelib.api.mvcc.result.PutResult;
import org.apache.bookkeeper.statelib.impl.journal.CommandProcessor;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoDeleteOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoIncrementOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoPutOpImpl;
import org.apache.bookkeeper.statestore.proto.Command;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class MVCCCommandProcessor implements CommandProcessor<MVCCStoreImpl<byte[], byte[]>> {

    public static MVCCCommandProcessor of() {
        return new MVCCCommandProcessor();
    }

    private void applyPutCommand(long revision, Command command, MVCCStoreImpl<byte[], byte[]> store) {
        ProtoPutOpImpl op = ProtoPutOpImpl.newPutOp(revision, command);
        try {
            applyPutOp(revision, op, true, store);
        } finally {
            op.recycle();
        }
    }

    private void applyPutOp(long revision,
                            PutOp<byte[], byte[]> op,
                            boolean ignoreSmallerRevision,
                            MVCCStoreImpl<byte[], byte[]> localStore) {
        PutResult<byte[], byte[]> result = localStore.put(revision, op);
        try {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        } finally {
            result.recycle();
        }
    }

    private void applyDeleteCommand(long revision, Command command,
                                    MVCCStoreImpl<byte[], byte[]> store) {
        ProtoDeleteOpImpl op = ProtoDeleteOpImpl.newDeleteOp(revision, command.getDeleteReq());
        try {
            applyDeleteOp(revision, op, true, store);
        } finally {
            op.recycle();
        }
    }

    private void applyDeleteOp(long revision,
                               DeleteOp<byte[], byte[]> op,
                               boolean ignoreSmallerRevision,
                               MVCCStoreImpl<byte[], byte[]> localStore) {
        DeleteResult<byte[], byte[]> result = localStore.delete(revision, op);
        try {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        } finally {
            result.recycle();
        }
    }

    private void applyTxnCommand(long revision, Command command,
                                 MVCCStoreImpl<byte[], byte[]> store) {
        throw new UnsupportedOperationException();
    }

    private void applyIncrCommand(long revision, Command command,
                                  MVCCStoreImpl<byte[], byte[]> store) {
        ProtoIncrementOpImpl op = ProtoIncrementOpImpl.newIncrementOp(revision, command);
        try {
            applyIncrOp(revision, op, true, store);
        } finally {
            op.recycle();
        }
    }

    private void applyIncrOp(long revision,
                             IncrementOp<byte[], byte[]> op,
                             boolean ignoreSmallerRevision,
                             MVCCStoreImpl<byte[], byte[]> localStore) {
        IncrementResult<byte[], byte[]> result = localStore.increment(revision, op);
        try {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        } finally {
            result.recycle();
        }
    }

    @Override
    public void applyCommand(long txid, ByteBuf cmdBuf, MVCCStoreImpl<byte[], byte[]> store) {
        Command command = MVCCUtils.newCommand(cmdBuf);
        switch (command.getReqCase()) {
            case NOP_REQ:
                return;
            case PUT_REQ:
                applyPutCommand(txid, command, store);
                return;
            case DELETE_REQ:
                applyDeleteCommand(txid, command, store);
                return;
            case TXN_REQ:
                applyTxnCommand(txid, command, store);
                return;
            case INCR_REQ:
                applyIncrCommand(txid, command, store);
                return;
            default:
                return;
        }
    }
}
