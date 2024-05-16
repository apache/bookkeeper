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
package org.apache.bookkeeper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.junit.jupiter.api.Test;

/**
 * Unit test of {@link BookieAddressResolverDisabled}.
 */
public class BookieAddressResolverDisabledTest {

    @Test
    public void testResolve() {
        BookieAddressResolver resolver = new BookieAddressResolverDisabled();

        BookieSocketAddress addr1 = resolver.resolve(BookieId.parse("127.0.0.1:3181"));
        assertEquals("127.0.0.1", addr1.getHostName());
        assertEquals(3181, addr1.getPort());

        BookieSocketAddress addr2 = resolver.resolve(BookieId.parse("localhost:3182"));
        assertEquals("localhost", addr2.getHostName());
        assertEquals(3182, addr2.getPort());

        try {
            resolver.resolve(BookieId.parse("foobar"));
            fail("Non-legacy style bookie id should fail to resolve address");
        } catch (Exception e) {
            assertTrue(e instanceof BookieAddressResolver.BookieIdNotResolvedException);
        }
    }

}
