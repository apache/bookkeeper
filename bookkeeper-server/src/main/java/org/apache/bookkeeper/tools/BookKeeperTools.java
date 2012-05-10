package org.apache.bookkeeper.tools;

/*
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

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import java.net.InetSocketAddress;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BKException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides Admin Tools to manage the BookKeeper cluster.
 *
 */
public class BookKeeperTools {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperTools.class);

    /**
     * Main method so we can invoke the bookie recovery via command line.
     *
     * @param args
     *            Arguments to BookKeeperTools. 2 are required and the third is
     *            optional. The first is a comma separated list of ZK server
     *            host:port pairs. The second is the host:port socket address
     *            for the bookie we are trying to recover. The third is the
     *            host:port socket address of the optional destination bookie
     *            server we want to replicate the data over to.
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BKException
     */
    public static void main(String[] args) 
            throws InterruptedException, IOException, KeeperException, BKException {
        // Validate the inputs
        if (args.length < 2) {
            System.err.println("USAGE: BookKeeperTools zkServers bookieSrc [bookieDest]");
            return;
        }
        // Parse out the input arguments
        String zkServers = args[0];
        String bookieSrcString[] = args[1].split(":");
        if (bookieSrcString.length < 2) {
            System.err.println("BookieSrc inputted has invalid name format (host:port expected): " + args[1]);
            return;
        }
        final InetSocketAddress bookieSrc = new InetSocketAddress(bookieSrcString[0], Integer
                .parseInt(bookieSrcString[1]));
        InetSocketAddress bookieDest = null;
        if (args.length < 3) {
            String bookieDestString[] = args[2].split(":");
            if (bookieDestString.length < 2) {
                System.err.println("BookieDest inputted has invalid name format (host:port expected): "
                                   + args[2]);
                return;
            }
            bookieDest = new InetSocketAddress(bookieDestString[0], Integer.parseInt(bookieDestString[1]));
        }

        // Create the BookKeeperTools instance and perform the bookie recovery
        // synchronously.
        BookKeeperAdmin bkTools = new BookKeeperAdmin(zkServers);
        bkTools.recoverBookieData(bookieSrc, bookieDest);

        // Shutdown the resources used in the BookKeeperTools instance.
        bkTools.close();
    }

}
