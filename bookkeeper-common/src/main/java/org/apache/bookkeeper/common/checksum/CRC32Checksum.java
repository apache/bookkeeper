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
package org.apache.bookkeeper.common.checksum;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Delegate operations to java {@link CRC32}.
 */
class CRC32Checksum implements Checksum {

    private final CRC32 crc32;

    CRC32Checksum() {
        this.crc32 = new CRC32();
    }

    @Override
    public void update(byte[] b) {
        crc32.update(b);
    }

    @Override
    public void update(ByteBuffer buffer) {
        crc32.update(buffer);
    }

    @Override
    public void update(int b) {
        crc32.update(b);
    }

    @Override
    public void update(byte[] b, int off, int len) {
        crc32.update(b, off, len);
    }

    @Override
    public long getValue() {
        return crc32.getValue();
    }

    @Override
    public void reset() {
        crc32.reset();
    }
}
