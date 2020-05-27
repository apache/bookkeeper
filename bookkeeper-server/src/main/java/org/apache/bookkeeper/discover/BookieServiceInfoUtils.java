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
package org.apache.bookkeeper.discover;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Utility class for {@link BookieServiceInfo}.
 */
public final class BookieServiceInfoUtils {

    /**
     * Creates a default legacy bookie info implementation.
     * In the default implementation there is one endpoint with
     * <code>bookie-rpc</code> protocol and the bookie id in the host port.
     *
     * @param bookieId bookie id
     * @return default implementation of a BookieServiceInfo
     * @throws UnknownHostException if the given bookieId is invalid
     */
    public static BookieServiceInfo buildLegacyBookieServiceInfo(String bookieId) throws UnknownHostException {
        BookieSocketAddress address = new BookieSocketAddress(bookieId);
        BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
        endpoint.setId(bookieId);
        endpoint.setHost(address.getHostName());
        endpoint.setPort(address.getPort());
        endpoint.setProtocol("bookie-rpc");
        endpoint.setAuth(Collections.emptyList());
        endpoint.setExtensions(Collections.emptyList());
        return new BookieServiceInfo(Collections.emptyMap(), Arrays.asList(endpoint));
    }

}
