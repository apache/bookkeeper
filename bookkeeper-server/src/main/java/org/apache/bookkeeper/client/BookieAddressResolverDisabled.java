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
package org.apache.bookkeeper.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;

/**
 * Resolve legacy style BookieIDs to Network addresses.
 */
@Slf4j
public final class BookieAddressResolverDisabled implements BookieAddressResolver {

    public BookieAddressResolverDisabled() {
    }

    @Override
    public BookieSocketAddress resolve(BookieId bookieId) {
        return BookieSocketAddress.resolveLegacyBookieId(bookieId);
    }

}
