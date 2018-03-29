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
package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;

import java.util.zip.CRC32;

import org.apache.bookkeeper.proto.checksum.CRC32DigestManager.CRC32Digest;

/**
 * Regular implementation of CRC32 digest that makes use of {@link CRC32} class.
 */
class StandardCRC32Digest implements CRC32Digest {

    private final CRC32 crc = new CRC32();

    @Override
    public long getValueAndReset() {
        long value = crc.getValue();
        crc.reset();
        return value;
    }

    @Override
    public void update(ByteBuf buf) {
        crc.update(buf.nioBuffer());
    }
}
