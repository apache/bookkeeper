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

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.common.hash.Murmur3;

/**
 * The base hash router.
 */
public abstract class AbstractHashRouter<K> implements HashRouter<K> {

    private static final long serialVersionUID = -7979076779920023308L;
    public static final long HASH_SEED = 383242705L;

    protected AbstractHashRouter() {
    }

    @Override
    public Long getRoutingKey(K key) {
        ByteBuf keyData = getRoutingKeyData(key);
        try {
            return Murmur3.hash128(
                keyData, keyData.readerIndex(), keyData.readableBytes(), HASH_SEED)[0];
        } finally {
            ReferenceCountUtil.safeRelease(keyData);
        }
    }

    abstract ByteBuf getRoutingKeyData(K key);
}
