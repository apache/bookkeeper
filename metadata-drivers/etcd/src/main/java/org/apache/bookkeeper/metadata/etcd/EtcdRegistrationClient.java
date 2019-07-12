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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.google.common.collect.Maps;
import java.net.UnknownHostException;
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
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Etcd based registration client.
 */
@Slf4j
class EtcdRegistrationClient implements RegistrationClient {

    private static Function<ByteSequence, BookieSocketAddress> newBookieSocketAddressFunc(String prefix) {
        return bs -> {
            String addrStr = bs.toStringUtf8();
            try {
                return new BookieSocketAddress(addrStr.replace(prefix, ""));
            } catch (UnknownHostException e) {
                throw new RuntimeException("Unknown bookie address '" + addrStr + "' : ", e);
            }
        };
    }

    private final EtcdWatchClient watchClient;
    private final KeySetReader<BookieSocketAddress> writableBookiesReader;
    private final KeySetReader<BookieSocketAddress> readonlyBookiesReader;
    private Map<RegistrationListener, Consumer<Versioned<Set<BookieSocketAddress>>>> writableListeners =
        Maps.newHashMap();
    private Map<RegistrationListener, Consumer<Versioned<Set<BookieSocketAddress>>>> readonlyListeners =
        Maps.newHashMap();

    EtcdRegistrationClient(String scope,
                           Client client) {
        this.watchClient = new EtcdWatchClient(client);
        this.writableBookiesReader = new KeySetReader<>(
            client,
            watchClient,
            newBookieSocketAddressFunc(EtcdUtils.getWritableBookiesBeginPath(scope)),
            ByteSequence.fromString(EtcdUtils.getWritableBookiesBeginPath(scope)),
            ByteSequence.fromString(EtcdUtils.getWritableBookiesEndPath(scope))
        );
        this.readonlyBookiesReader = new KeySetReader<>(
            client,
            watchClient,
            newBookieSocketAddressFunc(EtcdUtils.getReadonlyBookiesBeginPath(scope)),
            ByteSequence.fromString(EtcdUtils.getReadonlyBookiesBeginPath(scope)),
            ByteSequence.fromString(EtcdUtils.getReadonlyBookiesEndPath(scope))
        );
    }


    @Override
    public void close() {
        this.writableBookiesReader.close();
        this.readonlyBookiesReader.close();
        this.watchClient.close();
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return writableBookiesReader.read();
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getAllBookies() {
        return FutureUtils.exception(new BKException.BKIllegalOpException());
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return readonlyBookiesReader.read();
    }

    private static CompletableFuture<Void> registerListener(
        KeySetReader<BookieSocketAddress> keySetReader,
        Map<RegistrationListener, Consumer<Versioned<Set<BookieSocketAddress>>>> listeners,
        RegistrationListener listener
    ) {
        Consumer<Versioned<Set<BookieSocketAddress>>> consumer;
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
        KeySetReader<BookieSocketAddress> keySetReader,
        Map<RegistrationListener, Consumer<Versioned<Set<BookieSocketAddress>>>> listeners,
        RegistrationListener listener
    ) {
        Consumer<Versioned<Set<BookieSocketAddress>>> consumer = listeners.get(listener);
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
