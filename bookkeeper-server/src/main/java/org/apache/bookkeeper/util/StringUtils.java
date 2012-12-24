package org.apache.bookkeeper.util;

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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Provided utilites for parsing network addresses, ledger-id from node paths
 * etc.
 *
 */
public class StringUtils {

    // Ledger Node Prefix
    static public final String LEDGER_NODE_PREFIX = "L";

    /**
     * Parses address into IP and port.
     *
     * @param addr
     *            String
     */

    public static InetSocketAddress parseAddr(String s) throws IOException {

        String parts[] = s.split(":");
        if (parts.length != 2) {
            throw new IOException(s + " does not have the form host:port");
        }
        int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IOException(s + " does not have the form host:port");
        }

        InetSocketAddress addr = new InetSocketAddress(parts[0], port);
        return addr;
    }

    public static String addrToString(InetSocketAddress addr) {
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }

    /**
     * Formats ledger ID according to ZooKeeper rules
     *
     * @param id
     *            znode id
     */
    public static String getZKStringId(long id) {
        return String.format("%010d", id);
    }

    /**
     * Get the hierarchical ledger path according to the ledger id
     *
     * @param ledgerId
     *          ledger id
     * @return the hierarchical path
     */
    public static String getHierarchicalLedgerPath(long ledgerId) {
        String ledgerIdStr = getZKStringId(ledgerId);
        // do 2-4-4 split
        StringBuilder sb = new StringBuilder();
        sb.append("/")
          .append(ledgerIdStr.substring(0, 2)).append("/")
          .append(ledgerIdStr.substring(2, 6)).append("/")
          .append(LEDGER_NODE_PREFIX)
          .append(ledgerIdStr.substring(6, 10));
        return sb.toString();
    }

    /**
     * Parse the hierarchical ledger path to its ledger id
     *
     * @param hierarchicalLedgerPath
     * @return the ledger id
     * @throws IOException
     */
    public static long stringToHierarchicalLedgerId(String hierarchicalLedgerPath)
            throws IOException {
        String[] hierarchicalParts = hierarchicalLedgerPath.split("/");
        if (hierarchicalParts.length != 3) {
            throw new IOException("it is not a valid hierarchical path name : " + hierarchicalLedgerPath);
        }
        hierarchicalParts[2] =
            hierarchicalParts[2].substring(LEDGER_NODE_PREFIX.length());
        return stringToHierarchicalLedgerId(hierarchicalParts);
    }

    /**
     * Get ledger id
     *
     * @param levelNodes
     *          level of the ledger path
     * @return ledger id
     * @throws IOException
     */
    public static long stringToHierarchicalLedgerId(String...levelNodes) throws IOException {
        try {
            StringBuilder sb = new StringBuilder();
            for (String node : levelNodes) {
                sb.append(node);
            }
            return Long.parseLong(sb.toString());
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
    }

}
