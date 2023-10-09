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
 *
 */
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.proto.MockBookieClient;

/**
 * Adapter for tests to get the public access from LedgerHandle for its default
 * scope.
 */
public class LedgerHandleAdapter {

    public static ByteBuf toSend(LedgerHandle lh, long entryId, ByteBuf data) {
        return MockBookieClient.copyData(lh.getDigestManager()
                .computeDigestAndPackageForSending(entryId, lh.getLastAddConfirmed(),
                        lh.addToLength(data.readableBytes()), data, new byte[20], 0));
    }
}
