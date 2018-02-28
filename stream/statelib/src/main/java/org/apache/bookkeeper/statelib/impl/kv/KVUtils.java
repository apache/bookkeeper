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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.proto.statestore.kv.Command;
import org.apache.bookkeeper.proto.statestore.kv.DeleteRequest;
import org.apache.bookkeeper.proto.statestore.kv.NopRequest;
import org.apache.bookkeeper.proto.statestore.kv.PutIfAbsentRequest;
import org.apache.bookkeeper.proto.statestore.kv.PutRequest;

/**
 * Utils for kv stores.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class KVUtils {

    static final Command NOP_CMD = Command.newBuilder()
        .setNopReq(NopRequest.newBuilder().build())
        .build();

    static ByteBuf serialize(ByteBuf valBuf, long revision) {
        int serializedSize = valBuf.readableBytes() + Long.BYTES;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(serializedSize);
        buffer.writeLong(revision);
        buffer.writeBytes(valBuf);
        return buffer;
    }

    static ByteBuf serialize(byte[] value, long revision) {
        int serializedSize = value.length + Long.BYTES;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(serializedSize);
        buffer.writeLong(revision);
        buffer.writeBytes(value);
        return buffer;
    }

    static <V> V deserialize(Coder<V> valCoder,
                             ByteBuf valBuf) {
        valBuf.skipBytes(Long.BYTES);
        return valCoder.decode(valBuf);
    }

    static Command newCommand(ByteBuf cmdBuf) throws InvalidProtocolBufferException {
        return Command.parseFrom(cmdBuf.nioBuffer());
    }

    static ByteBuf newCommandBuf(Command cmd) throws IOException {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(cmd.getSerializedSize());
        try {
            cmd.writeTo(new ByteBufOutputStream(buf));
        } catch (IOException e) {
            buf.release();
            throw e;
        }
        return buf;
    }

    static PutRequest newPutRequest(byte[] keyBytes,
                                    byte[] valBytes) {
        return PutRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
            .setValue(UnsafeByteOperations.unsafeWrap(valBytes))
            .build();
    }

    static PutIfAbsentRequest newPutIfAbsentRequest(byte[] keyBytes,
                                                    byte[] valBytes) {
        return PutIfAbsentRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
            .setValue(UnsafeByteOperations.unsafeWrap(valBytes))
            .build();
    }

    static DeleteRequest newDeleteRequest(byte[] keyBytes) {
        return DeleteRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(keyBytes))
            .build();
    }

}
