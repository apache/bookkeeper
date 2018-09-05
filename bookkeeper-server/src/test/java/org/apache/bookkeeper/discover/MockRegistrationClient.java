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

package org.apache.bookkeeper.discover;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Mock implementation of registration client.
 * All actions take place in a single thread executor, so they are async
 * w.r.t. the caller.
 */
public class MockRegistrationClient implements RegistrationClient {
    final ExecutorService executor;
    private long currentVersion = 0;
    private Set<BookieSocketAddress> bookies = new HashSet<BookieSocketAddress>();
    private Set<BookieSocketAddress> readOnlyBookies = new HashSet<BookieSocketAddress>();
    private Set<RegistrationListener> bookieWatchers = new HashSet<RegistrationListener>();
    private Set<RegistrationListener> readOnlyBookieWatchers = new HashSet<RegistrationListener>();

    public MockRegistrationClient() {
        this.executor = Executors.newSingleThreadExecutor((r) -> new Thread(r, "MockRegistrationClient"));
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private static Versioned<Set<BookieSocketAddress>> versioned(Set<BookieSocketAddress> bookies, long version) {
        return new Versioned<>(Collections.unmodifiableSet(bookies), new LongVersion(version));
    }

    public CompletableFuture<Void> addBookies(BookieSocketAddress... bookies) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                currentVersion++;
                for (BookieSocketAddress b : bookies) {
                    this.bookies.add(b);
                }
                bookieWatchers.forEach(w -> w.onBookiesChanged(versioned(this.bookies, currentVersion)));
                promise.complete(null);
            });
        return promise;
    }

    public CompletableFuture<Void> removeBookies(BookieSocketAddress... bookies) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                currentVersion++;
                for (BookieSocketAddress b : bookies) {
                    this.bookies.add(b);
                }
                bookieWatchers.forEach(w -> w.onBookiesChanged(versioned(this.bookies, currentVersion)));
                promise.complete(null);
            });
        return promise;
    }

    public CompletableFuture<Void> addReadOnlyBookies(BookieSocketAddress... bookies) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                currentVersion++;
                for (BookieSocketAddress b : bookies) {
                    this.readOnlyBookies.add(b);
                }
                readOnlyBookieWatchers.forEach(w -> w.onBookiesChanged(versioned(readOnlyBookies, currentVersion)));
                promise.complete(null);
            });
        return promise;
    }

    public CompletableFuture<Void> removeReadOnlyBookies(BookieSocketAddress... bookies) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                currentVersion++;
                for (BookieSocketAddress b : bookies) {
                    this.readOnlyBookies.add(b);
                }
                readOnlyBookieWatchers.forEach(w -> w.onBookiesChanged(versioned(readOnlyBookies, currentVersion)));
                promise.complete(null);
            });
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        CompletableFuture<Versioned<Set<BookieSocketAddress>>> promise = new CompletableFuture<>();
        executor.submit(() -> promise.complete(versioned(bookies, currentVersion)));
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        CompletableFuture<Versioned<Set<BookieSocketAddress>>> promise = new CompletableFuture<>();
        executor.submit(() -> promise.complete(versioned(readOnlyBookies, currentVersion)));
        return promise;
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                bookieWatchers.add(listener);
                promise.complete(null);
            });
        return promise;
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener listener) {
        executor.submit(() -> {
                bookieWatchers.remove(listener);
            });
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        executor.submit(() -> {
                readOnlyBookieWatchers.add(listener);
                promise.complete(null);
            });
        return promise;
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener listener) {
        executor.submit(() -> {
                readOnlyBookieWatchers.remove(listener);
            });
    }
}
