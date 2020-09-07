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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import java.util.UUID;
import org.junit.Test;

/**
 * Unit tests for BookieId class.
 */
public class BookieIdTest {

    @Test
    public void testToString() {
        assertEquals("test", BookieId.parse("test").toString());
    }

    @Test
    public void testParse() {
        assertEquals("test", BookieId.parse("test").getId());
    }

    @Test
    public void testEquals() {
        assertEquals(BookieId.parse("test"), BookieId.parse("test"));
        assertNotEquals(BookieId.parse("test"), BookieId.parse("test2"));
    }

    @Test
    public void testHashcode() {
        assertEquals(BookieId.parse("test").hashCode(), BookieId.parse("test").hashCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate1() {
        BookieId.parse("non valid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate2() {
        BookieId.parse("non$valid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateReservedWord() {
        // 'readonly' is a reserved word for the ZK based implementation
        BookieId.parse("readonly");
    }

    @Test
    public void testValidateHostnamePort() {
        BookieId.parse("this.is.an.hostname:1234");
    }

    @Test
    public void testValidateIPv4Port() {
        BookieId.parse("1.2.3.4:1234");
    }

    @Test
    public void testValidateUUID() {
        BookieId.parse(UUID.randomUUID().toString());
    }

    @Test
    public void testWithDashAndUnderscore() {
        BookieId.parse("testRegisterUnregister_ReadonlyBookie-readonly:3181");
    }

}
