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

import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * A central place for creating and managing mvcc stores.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MVCCStores {

    /**
     * Provide a supplier to supply mvcc stores with a given namespace supplier.
     *
     * @param nsSupplier namespace supplier
     * @return a supplier that supplies mvcc stores.
     */
    public static Supplier<MVCCAsyncStore<byte[], byte[]>> bytesStoreSupplier(Supplier<Namespace> nsSupplier) {
        return () -> new MVCCAsyncBytesStoreImpl(
            () -> new MVCCStoreImpl<>(),
            nsSupplier
        );
    }

}
