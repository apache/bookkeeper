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
package org.apache.distributedlog.statelib.impl.mvcc;

import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.distributedlog.statelib.api.exceptions.MVCCStoreException;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOp;
import org.apache.distributedlog.statelib.api.mvcc.result.Code;
import org.apache.distributedlog.statelib.api.mvcc.result.PutResult;
import org.apache.distributedlog.statelib.impl.journal.CommandProcessor;
import org.apache.distributedlog.statelib.impl.mvcc.op.proto.ProtoPutOpImpl;
import org.apache.distributedlog.statestore.proto.Command;

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

    private void applyDeleteCommand(long revision, Command command) {
        throw new UnsupportedOperationException();
    }

    private void applyTxnCommand(long revision, Command command) {
        throw new UnsupportedOperationException();
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
                applyDeleteCommand(txid, command);
                return;
            case TXN_REQ:
                applyTxnCommand(txid, command);
                return;
            default:
                return;
        }
    }
}
