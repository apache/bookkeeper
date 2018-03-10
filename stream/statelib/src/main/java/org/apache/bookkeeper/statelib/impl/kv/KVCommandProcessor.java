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
package org.apache.bookkeeper.statelib.impl.kv;

import static org.apache.bookkeeper.statelib.impl.kv.KVUtils.newCommand;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.proto.statestore.kv.Command;
import org.apache.bookkeeper.proto.statestore.kv.DeleteRequest;
import org.apache.bookkeeper.proto.statestore.kv.PutRequest;
import org.apache.bookkeeper.statelib.impl.journal.CommandProcessor;

/**
 * A command processor to apply commands happened to the kv state store.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class KVCommandProcessor implements CommandProcessor<RocksdbKVStore<byte[], byte[]>> {

    public static KVCommandProcessor of() {
        return new KVCommandProcessor();
    }

    @Override
    public void applyCommand(long revision, ByteBuf cmdBuf, RocksdbKVStore<byte[], byte[]> store) {
        Command command;
        try {
            command = newCommand(cmdBuf);
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid kv command found : buffer = {}, txid = {}", cmdBuf, revision);
            // TODO: better to handle this
            return;
        }

        switch (command.getReqCase()) {
            case NOP_REQ:
                break;
            case PUT_IF_ABSENT_REQ:
                applyPutIfAbsentCommand(revision, command, store);
                return;
            case PUT_REQ:
                applyPutCommand(revision, command, store);
                return;
            case DEL_REQ:
                applyDeleteCommand(revision, command, store);
                return;
            default:
                return;
        }
    }

    private void applyPutCommand(long revision, Command command, RocksdbKVStore<byte[], byte[]> store) {
        PutRequest putReq = command.getPutReq();
        byte[] keyBytes = putReq.getKey().toByteArray();
        byte[] valBytes = putReq.getValue().toByteArray();
        @Cleanup("release") ByteBuf serializedValBuf = KVUtils.serialize(valBytes, revision);
        byte[] serializedValBytes = ByteBufUtil.getBytes(serializedValBuf);
        store.put(keyBytes, serializedValBytes, revision);
    }

    private void applyPutIfAbsentCommand(long revision, Command command, RocksdbKVStore<byte[], byte[]> store) {
        PutRequest putReq = command.getPutReq();
        byte[] keyBytes = putReq.getKey().toByteArray();
        byte[] valBytes = putReq.getValue().toByteArray();
        @Cleanup("release") ByteBuf serializedValBuf = KVUtils.serialize(valBytes, revision);
        byte[] serializedValBytes = ByteBufUtil.getBytes(serializedValBuf);
        store.putIfAbsent(keyBytes, serializedValBytes, revision);
    }

    private void applyDeleteCommand(long revision, Command command, RocksdbKVStore<byte[], byte[]> store) {
        DeleteRequest delReq = command.getDelReq();
        byte[] keyBytes = delReq.getKey().toByteArray();
        store.delete(keyBytes, revision);
    }
}
