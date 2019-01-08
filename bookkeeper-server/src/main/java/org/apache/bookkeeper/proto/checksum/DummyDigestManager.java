/**
 *
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
 *
 */
package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;


/**
 * This class provides a noop digest implementation.
 */
public class DummyDigestManager extends DigestManager {
    public DummyDigestManager(long ledgerId, boolean useV2Protocol, ByteBufAllocator allocator) {
        super(ledgerId, useV2Protocol, allocator);
    }

    @Override
    int getMacCodeLength() {
        return 0;
    }

    @Override
    void update(ByteBuf buffer) {}

    @Override
    void populateValueAndReset(ByteBuf buffer) {}
}
