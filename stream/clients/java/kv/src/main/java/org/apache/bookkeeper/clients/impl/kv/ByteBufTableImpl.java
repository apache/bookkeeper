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
package org.apache.bookkeeper.clients.impl.kv;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.Txn;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;

/**
 * The default implementation of {@link Table}.
 */
public class ByteBufTableImpl implements Table<ByteBuf, ByteBuf> {

    private final PTable<ByteBuf, ByteBuf> underlying;

    public ByteBufTableImpl(PTable<ByteBuf, ByteBuf> underlying) {
        this.underlying = underlying;
    }

    @Override
    public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(ByteBuf key, RangeOption<ByteBuf> option) {
        return underlying.get(key, key, option);
    }

    @Override
    public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(ByteBuf key, ByteBuf value, PutOption<ByteBuf> option) {
        return underlying.put(key, key, value, option);
    }

    @Override
    public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(ByteBuf key, DeleteOption<ByteBuf> option) {
        return underlying.delete(key, key, option);
    }

    @Override
    public CompletableFuture<IncrementResult<ByteBuf, ByteBuf>> increment(ByteBuf key,
                                                                          long amount,
                                                                          IncrementOption<ByteBuf> option) {
        return underlying.increment(key, key, amount, option);
    }

    @Override
    public Txn<ByteBuf, ByteBuf> txn(ByteBuf key) {
        return underlying.txn(key);
    }

    @Override
    public OpFactory<ByteBuf, ByteBuf> opFactory() {
        return underlying.opFactory();
    }

    @Override
    public void close() {
        underlying.close();
    }
}
