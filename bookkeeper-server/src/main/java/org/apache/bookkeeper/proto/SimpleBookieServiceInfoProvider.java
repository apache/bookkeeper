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
package org.apache.bookkeeper.proto;

import java.net.UnknownHostException;
import java.util.function.Supplier;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfoUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Simple Implementation of BookieServiceInfo supplier.
 */
public class SimpleBookieServiceInfoProvider implements Supplier<BookieServiceInfo> {
    private final BookieSocketAddress bookieSocketAddress;

    public SimpleBookieServiceInfoProvider(ServerConfiguration serverConfiguration) {
        try {
            this.bookieSocketAddress = Bookie.getBookieAddress(serverConfiguration);
        } catch (UnknownHostException err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public BookieServiceInfo get() {
        try {
            return BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieSocketAddress.toBookieId().toString());
        } catch (UnknownHostException err) {
            throw new RuntimeException(err);
        }
    }

}
