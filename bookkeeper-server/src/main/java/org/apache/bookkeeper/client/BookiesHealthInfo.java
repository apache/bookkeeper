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

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * This class encompasses heuristics used to determine the health of a Bookkeeper server for read
 * ordering. Currently we use the history of failed entries and length of the pending requests queue.
 */
public class BookiesHealthInfo {

  private Map<BookieSocketAddress, Long> bookieFailureHistory;
  private Map<BookieSocketAddress, Integer> bookiePendingRequests;

  BookiesHealthInfo() {
    this(new HashMap<BookieSocketAddress, Long>(), new HashMap<BookieSocketAddress, Integer>());
  }

  BookiesHealthInfo(Map<BookieSocketAddress, Long> bookieFailureHistory, Map<BookieSocketAddress, Integer> bookiePendingRequests) {
    this.bookieFailureHistory = bookieFailureHistory;
    this.bookiePendingRequests = bookiePendingRequests;
  }

  /**
   * Return the failure history for a bookie.
   *
   * @param bookieSocketAddress
   * @return failed entries on a bookie, -1 if there has been no failures.
   */
  public long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress) {
    return bookieFailureHistory.getOrDefault(bookieSocketAddress, -1L);
  }

  /**
   * Returns pending requests to a bookie.
   * @param bookieSocketAddress
   * @return number of pending requests
   */
  public int getBookiePendingRequests(BookieSocketAddress bookieSocketAddress) {
    return bookiePendingRequests.getOrDefault(bookieSocketAddress, 0);
  }

}
