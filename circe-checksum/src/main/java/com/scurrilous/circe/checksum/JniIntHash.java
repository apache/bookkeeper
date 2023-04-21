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
package com.scurrilous.circe.checksum;

import static com.scurrilous.circe.params.CrcParameters.CRC32C;
import com.scurrilous.circe.IncrementalIntHash;
import io.netty.buffer.ByteBuf;

public class JniIntHash implements IntHash {

    private final IncrementalIntHash hash = new Crc32cSse42Provider().getIncrementalInt(CRC32C);

    @Override
    public int calculate(ByteBuf buffer) {
        return calculate(buffer, buffer.readerIndex(), buffer.readableBytes());
    }

    @Override
    public int resume(int current, ByteBuf buffer) {
        return resume(current, buffer, buffer.readerIndex(), buffer.readableBytes());
    }

    @Override
    public int calculate(ByteBuf buffer, int offset, int len) {
        return resume(0, buffer, offset, len);
    }

    @Override
    public int resume(int current, ByteBuf buffer, int offset, int len) {
        if (buffer.hasMemoryAddress()) {
            return hash.resume(current, buffer.memoryAddress() + offset, len);
        } else if (buffer.hasArray()) {
            return hash.resume(current, buffer.array(), buffer.arrayOffset() + offset, len);
        } else {
            return hash.resume(current, buffer.slice(offset, len).nioBuffer());
        }
    }
}
