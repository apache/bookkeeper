/**
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
package org.apache.bookkeeper.client;

import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;

/**
 * Resolved BookieIDs to Network addresses.
 */
@Slf4j
public class CachingBookieAddressResolver implements BookieAddressResolver {

    private final ConcurrentHashMap<BookieId, BookieSocketAddress> resolvedBookieAddressCache =
                                                                            new ConcurrentHashMap<>();
    private final RegistrationClient registrationClient;

    public CachingBookieAddressResolver(RegistrationClient registrationClient) {
        this.registrationClient = registrationClient;
    }

    public void invalidateBookieAddress(BookieId address) {
        resolvedBookieAddressCache.remove(address);
    }

    @Override
    public BookieSocketAddress resolve(BookieId bookieId) {
        BookieSocketAddress cached = resolvedBookieAddressCache.get(bookieId);
        if (cached != null) {
            return cached;
        }
        try {
            BookieServiceInfo info = FutureUtils.result(registrationClient.getBookieServiceInfo(bookieId)).getValue();
            BookieServiceInfo.Endpoint endpoint = info.getEndpoints()
                    .stream().filter(e -> e.getProtocol().equals("bookie-rpc")).findAny().orElse(null);
            if (endpoint == null) {
                throw new Exception("bookie " + bookieId + " does not publish a bookie-rpc endpoint");
            }
            BookieSocketAddress res = new BookieSocketAddress(endpoint.getHost(), endpoint.getPort());
            log.info("Resolved {} as {}", bookieId, res);
            resolvedBookieAddressCache.put(bookieId, res);
            return res;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BookieIdNotResolvedException(bookieId, ex);
        }
    }

}
