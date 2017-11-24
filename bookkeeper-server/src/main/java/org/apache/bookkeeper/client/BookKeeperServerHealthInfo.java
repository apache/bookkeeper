package org.apache.bookkeeper.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * This class encompasses heuristics used to determine the health of a Bookkeeper server for read
 * ordering. Currently we use the history of failed entries and length of the pending requests queue.
 */
public class BookKeeperServerHealthInfo {

  private Map<BookieSocketAddress, Long> bookieFailureHistory;
  private Map<BookieSocketAddress, Integer> bookiePendingRequests;

  BookKeeperServerHealthInfo() {
    this(new HashMap<BookieSocketAddress, Long>(), new HashMap<BookieSocketAddress, Integer>());
  }

  BookKeeperServerHealthInfo(Map<BookieSocketAddress, Long> bookieFailureHistory, Map<BookieSocketAddress, Integer> bookiePendingRequests) {
    this.bookieFailureHistory = bookieFailureHistory;
    this.bookiePendingRequests = bookiePendingRequests;
  }

  /**
   * Return the failure history for a bookie.
   *
   * @param bookieSocketAddress
   * @return failed entries on a bookie, -1 if there has been no failures.
   */
  long getBookieFailureHistory(BookieSocketAddress bookieSocketAddress) {
    return bookieFailureHistory.getOrDefault(bookieSocketAddress, -1L);
  }

  /**
   * Returns pending requests to a bookie.
   * @param bookieSocketAddress
   * @return number of pending requests
   */
  int getBookiePendingRequests(BookieSocketAddress bookieSocketAddress) {
    return bookiePendingRequests.getOrDefault(bookieSocketAddress, 0);
  }

}
