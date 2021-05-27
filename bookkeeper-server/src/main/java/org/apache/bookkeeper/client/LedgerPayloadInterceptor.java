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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

/**
 * Interface for the interceptors that may need to modify
 * data written to the ledger.
 */
public interface LedgerPayloadInterceptor {

    /**
     * Called after interceptor creation (once)
     * @param ctx - ClientContext
     * @param interceptorsCfg - configuration prefixed by "interceptor.lpi"
     *                        (without the prefix for keys)
     */
    void init(final ClientContext ctx, final Configuration interceptorsCfg);

    /**
     * Executed before creating the ledger.
     * Gives opportunity to modify ledger's custom metadata.
     * Modified metadata will be persisted.
     * @param customMetadata - ledger's custom metadata
     */
    void beforeCreate(final Map<String, byte[]> customMetadata);

    /**
     * Executed before adding entry to the ledger.
     * Gives opportunity to modify the payload.
     * @param customMetadata - ledger's custom metadata
     * @param payload - data being added to the ledger
     * @return modified payload or the original payload
     */
    ByteBuf beforeAdd(final Map<String, byte[]> customMetadata, final ByteBuf payload);

    /**
     * Executed after receiving the payload from the bookie.
     * Gives opportunity to modify the payload.
     * @param customMetadata - ledger's custom metadata
     * @param payload - data read from the bookie
     * @return modified payload or the original payload
     */
    ByteBuf afterRead(final Map<String, byte[]> customMetadata, final ByteBuf payload);

    /**
     * called after the client is closed / interceptor is no longer usable.
     */
    void close();
}
