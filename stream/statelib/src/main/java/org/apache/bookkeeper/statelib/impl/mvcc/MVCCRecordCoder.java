/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.statelib.impl.mvcc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.stream.proto.kv.store.KeyMeta;

/**
 * A coder for encoding and decoding {@link MVCCRecord}s.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class MVCCRecordCoder implements Coder<MVCCRecord> {

    public static MVCCRecordCoder of() {
        return INSTANCE;
    }

    private static final MVCCRecordCoder INSTANCE = new MVCCRecordCoder();


    private static KeyMeta toKeyMeta(MVCCRecord record) {
        return new KeyMeta()
            .setCreateRevision(record.getCreateRev())
            .setModRevision(record.getModRev())
            .setVersion(record.getVersion())
            .setValueType(record.getValueType())
            .setExpireTime(record.getExpireTime());
    }

    @Override
    public byte[] encode(MVCCRecord record) {
        KeyMeta meta = toKeyMeta(record);
        int metaLen = meta.getSerializedSize();
        int valLen = record.getValue().readableBytes();

        int totalLen =
            Integer.BYTES     // meta len
                + metaLen           // meta bytes
                + Integer.BYTES     // val len
                + valLen;           // val bytes

        // NOTE: currently rocksdb jni only supports `byte[]`
        //       we can improve this if rocksdb jni support ByteBuffer or ByteBuf
        byte[] data = new byte[totalLen];
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        buf.writerIndex(0);
        buf.writeInt(metaLen);
        try {
            meta.writeTo(buf);
        } catch (RuntimeException e) {
            ReferenceCountUtil.release(buf);
            throw new StateStoreRuntimeException("Failed to serialize key metadata", e);
        }
        buf.writeInt(valLen);
        buf.writeBytes(record.getValue().slice());
        ReferenceCountUtil.release(buf);

        return data;
    }

    @Override
    public void encode(MVCCRecord value, ByteBuf destBuf) {
        destBuf.writeBytes(encode(value));
    }

    @Override
    public int getSerializedSize(MVCCRecord record) {
        int metaLen = toKeyMeta(record).getSerializedSize();
        int valLen = record.getValue().readableBytes();

        return Integer.BYTES    // meta len
            + metaLen           // meta bytes
            + Integer.BYTES     // val len
            + valLen;           // val bytes
    }

    @Override
    public MVCCRecord decode(ByteBuf data) {
        ByteBuf copy = data.slice();

        int metaLen = copy.readInt();
        KeyMeta meta = new KeyMeta();
        try {
            meta.parseFrom(copy, metaLen);
        } catch (RuntimeException e) {
            throw new StateStoreRuntimeException("Failed to deserialize key metadata", e);
        }
        int valLen = copy.readInt();
        ByteBuf valBuf = copy.retainedSlice(copy.readerIndex(), valLen);

        MVCCRecord record = MVCCRecord.newRecord();
        record.setCreateRev(meta.getCreateRevision());
        record.setModRev(meta.getModRevision());
        record.setVersion(meta.getVersion());
        record.setExpireTime(meta.getExpireTime());
        record.setValue(valBuf, meta.getValueType());
        return record;
    }


}
