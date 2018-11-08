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
package org.apache.bookkeeper.net;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.junit.Test;

/**
 * Tests for BookieSocketAddress getSocketAddress cache logic.
 */

public class BookieSocketAddressTest {

    @Test
    public void testHostnameBookieId() throws Exception {
        BookieSocketAddress hostnameAddress = new BookieSocketAddress("localhost", 3181);
        InetSocketAddress inetSocketAddress1 = hostnameAddress.getSocketAddress();
        InetSocketAddress inetSocketAddress2 = hostnameAddress.getSocketAddress();
        assertFalse("InetSocketAddress should be recreated", inetSocketAddress1 == inetSocketAddress2);
    }

    @Test
    public void testIPAddressBookieId() throws Exception {
        BookieSocketAddress ipAddress = new BookieSocketAddress("127.0.0.1", 3181);
        InetSocketAddress inetSocketAddress1 = ipAddress.getSocketAddress();
        InetSocketAddress inetSocketAddress2 = ipAddress.getSocketAddress();
        assertTrue("InetSocketAddress should be cached", inetSocketAddress1 == inetSocketAddress2);
    }
}
