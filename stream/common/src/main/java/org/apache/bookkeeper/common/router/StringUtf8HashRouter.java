/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.router;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Compute a hash value for a utf8 string.
 */
public class StringUtf8HashRouter extends AbstractHashRouter<String> {

    public static HashRouter<String> of() {
        return ROUTER;
    }

    private static final long serialVersionUID = -4630305722027547668L;

    private static final StringUtf8HashRouter ROUTER = new StringUtf8HashRouter();

    private StringUtf8HashRouter() {
    }

    @Override
    public ByteBuf getRoutingKeyData(String key) {
        int keyLen = key.length();
        ByteBuf keyBuf = PooledByteBufAllocator.DEFAULT.buffer(keyLen);
        keyBuf.writeCharSequence(key, UTF_8);
        return keyBuf;
    }
}
