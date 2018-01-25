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
package org.apache.distributedlog.clients.impl.kv;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.kv.PTableWriter;
import org.apache.distributedlog.api.kv.TableWriter;

/**
 * The default implementation of {@link TableWriter}.
 */
public class ByteBufTableWriterImpl implements TableWriter<ByteBuf, ByteBuf> {

    private final PTableWriter<ByteBuf, ByteBuf> underlying;

    public ByteBufTableWriterImpl(PTableWriter<ByteBuf, ByteBuf> underlying) {
        this.underlying = underlying;
    }

    @Override
    public CompletableFuture<Void> write(long sequenceId, ByteBuf key, ByteBuf value) {
        return underlying.write(
            sequenceId,
            key,
            key,
            value);
    }

    @Override
    public CompletableFuture<Void> increment(long sequenceId, ByteBuf key, long amount) {
        return underlying.increment(
            sequenceId,
            key,
            key,
            amount);
    }

    @Override
    public void close() {
        // no-op
    }
}
