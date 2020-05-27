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

import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.LimitedPrivate;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A registration client, which the bookkeeper client will use to interact with registration service.
 */
@LimitedPrivate
@Evolving
public interface RegistrationClient extends AutoCloseable {

    /**
     * Listener to receive changes from the registration service.
     */
    interface RegistrationListener {

        void onBookiesChanged(Versioned<Set<BookieSocketAddress>> bookies);

    }

    @Override
    void close();

    /**
     * Get the list of writable bookie identifiers.
     *
     * @return a future represents the list of writable bookies.
     */
    CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies();

    /**
     * Get the list of all bookies identifiers.
     *
     * @return a future represents the list of writable bookies.
     */
    CompletableFuture<Versioned<Set<BookieSocketAddress>>> getAllBookies();

    /**
     * Get the list of readonly bookie identifiers.
     *
     * @return a future represents the list of readonly bookies.
     */
    CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies();

    /**
     * Get detailed information about the services exposed by a Bookie.
     * For old bookies it is expected to return an empty BookieServiceInfo structure.
     *
     * @param bookieId this is the id of the bookie, it can be computed from a {@link BookieSocketAddress}
     * @return a future represents the available information.
     *
     * @since 4.11
     */
    default CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(String bookieId) {
        try {
            BookieServiceInfo bookieServiceInfo = BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId);
            return FutureUtils.value(new Versioned<>(bookieServiceInfo, new LongVersion(-1)));
        } catch (UnknownHostException e) {
            return FutureUtils.exception(e);
        }
    }

    /**
     * Watch the changes of bookies.
     *
     * <p>The topology changes of bookies will be propagated to the provided <i>listener</i>.
     *
     * @param listener listener to receive the topology changes of bookies.
     * @return a future which completes when the bookies have been read for
     *         the first time
     */
    CompletableFuture<Void> watchWritableBookies(RegistrationListener listener);

    /**
     * Unwatch the changes of bookies.
     *
     * @param listener listener to receive the topology changes of bookies.
     */
    void unwatchWritableBookies(RegistrationListener listener);

    /**
     * Watch the changes of bookies.
     *
     * <p>The topology changes of bookies will be propagated to the provided <i>listener</i>.
     *
     * @param listener listener to receive the topology changes of bookies.
     * @return a future which completes when the bookies have been read for
     *         the first time
     */
    CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener);

    /**
     * Unwatch the changes of bookies.
     *
     * @param listener listener to receive the topology changes of bookies.
     */
    void unwatchReadOnlyBookies(RegistrationListener listener);

}
