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
package org.apache.bookkeeper.api;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;

/**
 * The stream storage client.
 */
@Public
@Evolving
public interface StorageClient extends AutoAsyncCloseable {

    /**
     * Open a {@link PTable} <tt>table</tt> under <tt>namespace</tt>.
     *
     * @param namespace namespace
     * @param table table name
     * @return a future represents the open result
     */
    CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String namespace, String table);

    /**
     * Open a {@link PTable} <tt>table</tt> under the default namespace of this client.
     * The default namespace is configured when creating {@link StorageClient}.
     *
     * @param table table name
     * @return a future represents the open result
     */
    CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String table);

    /**
     * Open a {@link Table} <tt>table</tt> under <tt>namespace</tt>.
     *
     * @param namespace namespace
     * @param table table name
     * @return a future represents the open result
     */
    CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String namespace, String table);

    /**
     * Open a {@link Table} <tt>table</tt> under <tt>namespace</tt>.
     * The default namespace is configured when creating {@link StorageClient}.
     *
     * @param table table name
     * @return a future represents the open result
     */
    CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String table);

}
