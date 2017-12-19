/**
 *
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
 *
 */
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * This interface returns heuristics used to determine the health of a Bookkeeper server for read
 * ordering.
 */
public interface BookiesHealthInfo {

    /**
     * Return the failure history for a bookie.
     *
     * @param bookieSocketAddress
     * @return failed entries on a bookie, -1 if there have been no failures
     */
    long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress);

    /**
     * Returns pending requests to a bookie.
     *
     * @param bookieSocketAddress
     * @return number of pending requests
     */
    long getBookiePendingRequests(BookieSocketAddress bookieSocketAddress);

}
