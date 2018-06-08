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
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.statelib.impl.journal.CommandProcessor;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoDeleteOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoIncrementOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoPutOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoTxnOpImpl;
import org.apache.bookkeeper.stream.proto.kv.store.Command;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class MVCCCommandProcessor implements CommandProcessor<MVCCStoreImpl<byte[], byte[]>> {

    public static MVCCCommandProcessor of() {
        return new MVCCCommandProcessor();
    }

    private void applyPutCommand(long revision, Command command, MVCCStoreImpl<byte[], byte[]> store) {
        try (ProtoPutOpImpl op = ProtoPutOpImpl.newPutOp(command)) {
            applyPutOp(revision, op, true, store);
        }
    }

    private void applyPutOp(long revision,
                            PutOp<byte[], byte[]> op,
                            boolean ignoreSmallerRevision,
                            MVCCStoreImpl<byte[], byte[]> localStore) {
        try (PutResult<byte[], byte[]> result = localStore.put(revision, op)) {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        }
    }

    private void applyDeleteCommand(long revision, Command command,
                                    MVCCStoreImpl<byte[], byte[]> store) {
        try (ProtoDeleteOpImpl op = ProtoDeleteOpImpl.newDeleteOp(command.getDeleteReq())) {
            applyDeleteOp(revision, op, true, store);
        }
    }

    private void applyDeleteOp(long revision,
                               DeleteOp<byte[], byte[]> op,
                               boolean ignoreSmallerRevision,
                               MVCCStoreImpl<byte[], byte[]> localStore) {
        try (DeleteResult<byte[], byte[]> result = localStore.delete(revision, op)) {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        }
    }

    private void applyTxnCommand(long revision, Command command,
                                 MVCCStoreImpl<byte[], byte[]> store) {
        try (ProtoTxnOpImpl op = ProtoTxnOpImpl.newTxnOp(command.getTxnReq())) {
            applyTxnOp(revision, op, true, store);
        }
    }

    private void applyTxnOp(long revision,
                            TxnOp<byte[], byte[]> op,
                            boolean ignoreSmallerRevision,
                            MVCCStoreImpl<byte[], byte[]> localStore) {
        try (TxnResult<byte[], byte[]> result = localStore.processTxn(revision, op)) {
            if (log.isDebugEnabled()) {
                log.debug("Result after applying transaction {} : {} - success = {}",
                    revision, result.code(), result.isSuccess());
            }
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
        }
    }

    private void applyIncrCommand(long revision, Command command,
                                  MVCCStoreImpl<byte[], byte[]> store) {
        try (ProtoIncrementOpImpl op = ProtoIncrementOpImpl.newIncrementOp(command)) {
            applyIncrOp(revision, op, true, store);
        }
    }

    private void applyIncrOp(long revision,
                             IncrementOp<byte[], byte[]> op,
                             boolean ignoreSmallerRevision,
                             MVCCStoreImpl<byte[], byte[]> localStore) {
        try (IncrementResult<byte[], byte[]> result = localStore.increment(revision, op)) {
            if (Code.OK == result.code()
                || (ignoreSmallerRevision && Code.SMALLER_REVISION == result.code())) {
                return;
            }
            throw new MVCCStoreException(result.code(),
                "Failed to apply command " + op + " at revision "
                    + revision + " to the state store " + localStore.name());
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
