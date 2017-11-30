package org.apache.bookkeeper.client;

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
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Test BookKeeperClient which allows access to members we don't
 * wish to expose in the public API.
 */
@Slf4j
public class BookKeeperTestClient extends BookKeeper {
    public BookKeeperTestClient(ClientConfiguration conf)
            throws IOException, InterruptedException, BKException {
        super(conf);
    }

    public ZooKeeper getZkHandle() {
        return super.getZkHandle();
    }

    public ClientConfiguration getConf() {
        return super.getConf();
    }

    public BookieClient getBookieClient() {
        return bookieClient;
    }

    public Future<?> waitForReadOnlyBookie(BookieSocketAddress b)
            throws Exception {
        return waitForBookieInSet(b, false);
    }

    public Future<?> waitForWritableBookie(BookieSocketAddress b)
            throws Exception {
        return waitForBookieInSet(b, true);
    }

    /**
     * Wait for bookie to appear in either the writable set of bookies,
     * or the read only set of bookies. Also ensure that it doesn't exist
     * in the other set before completing.
     */
    private Future<?> waitForBookieInSet(BookieSocketAddress b,
                                                       boolean writable) {
        log.info("Wait for {} to become {}",
                 b, writable ? "writable" : "readonly");

        CompletableFuture<Void> readOnlyFuture = new CompletableFuture<>();
        CompletableFuture<Void> writableFuture = new CompletableFuture<>();

        RegistrationListener readOnlyListener = (bookies) -> {
            boolean contains = bookies.getValue().contains(b);
            if ((!writable && contains) || (writable && !contains)) {
                readOnlyFuture.complete(null);
            }
        };
        RegistrationListener writableListener = (bookies) -> {
            boolean contains = bookies.getValue().contains(b);
            if ((writable && contains) || (!writable && !contains)) {
                writableFuture.complete(null);
            }
        };

        regClient.watchWritableBookies(writableListener);
        regClient.watchReadOnlyBookies(readOnlyListener);
        return CompletableFuture.allOf(writableFuture, readOnlyFuture);
    }
}
