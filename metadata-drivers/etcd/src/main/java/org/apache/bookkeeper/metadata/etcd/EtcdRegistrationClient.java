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
package org.apache.bookkeeper.metadata.etcd;

import com.google.common.collect.Maps;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.metadata.etcd.helpers.KeySetReader;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Etcd based registration client.
 */
@Slf4j
class EtcdRegistrationClient implements RegistrationClient {

    private static Function<ByteSequence, BookieId> newBookieSocketAddressFunc(String prefix) {
        return bs -> {
            String addrStr = bs.toString(StandardCharsets.UTF_8);
            return BookieId.parse(addrStr.replace(prefix, ""));
        };
    }

    private final EtcdWatchClient watchClient;
    private final KeySetReader<BookieId> writableBookiesReader;
    private final KeySetReader<BookieId> readonlyBookiesReader;
    private Map<RegistrationListener, Consumer<Versioned<Set<BookieId>>>> writableListeners =
        Maps.newHashMap();
    private Map<RegistrationListener, Consumer<Versioned<Set<BookieId>>>> readonlyListeners =
        Maps.newHashMap();

    EtcdRegistrationClient(String scope,
                           Client client) {
        this.watchClient = new EtcdWatchClient(client);
        this.writableBookiesReader = new KeySetReader<>(
            client,
            watchClient,
            newBookieSocketAddressFunc(EtcdUtils.getWritableBookiesBeginPath(scope)),
            ByteSequence.from(EtcdUtils.getWritableBookiesBeginPath(scope), StandardCharsets.UTF_8),
            ByteSequence.from(EtcdUtils.getWritableBookiesEndPath(scope), StandardCharsets.UTF_8)
        );
        this.readonlyBookiesReader = new KeySetReader<>(
            client,
            watchClient,
            newBookieSocketAddressFunc(EtcdUtils.getReadonlyBookiesBeginPath(scope)),
            ByteSequence.from(EtcdUtils.getReadonlyBookiesBeginPath(scope), StandardCharsets.UTF_8),
            ByteSequence.from(EtcdUtils.getReadonlyBookiesEndPath(scope), StandardCharsets.UTF_8)
        );
    }


    @Override
    public void close() {
        this.writableBookiesReader.close();
        this.readonlyBookiesReader.close();
        this.watchClient.close();
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
        return writableBookiesReader.read();
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        return FutureUtils.exception(new BKException.BKIllegalOpException());
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return readonlyBookiesReader.read();
    }

    private static CompletableFuture<Void> registerListener(
        KeySetReader<BookieId> keySetReader,
        Map<RegistrationListener, Consumer<Versioned<Set<BookieId>>>> listeners,
        RegistrationListener listener
    ) {
        Consumer<Versioned<Set<BookieId>>> consumer;
        synchronized (listeners) {
            consumer = listeners.get(listener);
            if (null != consumer) {
                // already registered
                return FutureUtils.Void();
            } else {
                consumer = bookies -> listener.onBookiesChanged(bookies);
                listeners.put(listener, consumer);
            }
        }
        return keySetReader
            .readAndWatch(consumer)
            .thenApply(ignored -> null);
    }

    private static CompletableFuture<Void> unregisterListener(
        KeySetReader<BookieId> keySetReader,
        Map<RegistrationListener, Consumer<Versioned<Set<BookieId>>>> listeners,
        RegistrationListener listener
    ) {
        Consumer<Versioned<Set<BookieId>>> consumer = listeners.get(listener);
        if (null == consumer) {
            return FutureUtils.Void();
        } else {
            return keySetReader.unwatch(consumer);
        }
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        return registerListener(
            writableBookiesReader,
            writableListeners,
            listener
        );
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener listener) {
        unregisterListener(
            writableBookiesReader,
            writableListeners,
            listener
        );
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        return registerListener(
            readonlyBookiesReader,
            readonlyListeners,
            listener
        );
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener listener) {
        unregisterListener(
            readonlyBookiesReader,
            readonlyListeners,
            listener
        );
    }

}
