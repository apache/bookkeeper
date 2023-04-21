/*
 * Copyright 2020 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Maps a logical BookieId to a ResolvedBookieSocketAddress
 that it to a network address.
 */
public interface BookieAddressResolver {

    /**
     * Maps a logical address to a network address.
     * @param bookieId
     * @return a mapped address.
     * @throws BookieIdNotResolvedException if it is not possible to resolve the address of the BookieId
     */
    BookieSocketAddress resolve(BookieId bookieId) throws BookieIdNotResolvedException;

    /**
     * This error happens when there is not enough information to resolve a BookieId
     * to a BookieSocketAddress, this can happen when the Bookie is down
     * and it is not publishing its EndpointInfo.
     */
    class BookieIdNotResolvedException extends RuntimeException {
        private final BookieId bookieId;

        public BookieIdNotResolvedException(BookieId bookieId, Throwable cause) {
            super("Cannot resolve bookieId " + bookieId + ", bookie does not exist or it is not running", cause);
            this.bookieId = bookieId;
        }

        public BookieId getBookieId() {
            return bookieId;
        }

    }
}
