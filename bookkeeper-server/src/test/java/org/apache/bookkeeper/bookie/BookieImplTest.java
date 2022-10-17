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
package org.apache.bookkeeper.bookie;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing BookieImpl cases.
 */
public class BookieImplTest {

    @Test
    public void testMasterKeyCache() {
        byte[] masterKey1 = "key1".getBytes(UTF_8);
        long l1 = 100001;
        long l2 = 100002;
        byte[] masterKey2 = "key2".getBytes(UTF_8);
        long l3 = 100003;
        long l4 = 100004;

        // test BookieImpl.updateMasterKeyCache
        Assert.assertTrue(BookieImpl.addMasterKeyCache(masterKey1, l1));
        Assert.assertTrue(BookieImpl.addMasterKeyCache(masterKey1, l2));
        Assert.assertTrue(BookieImpl.addMasterKeyCache(masterKey2, l3));
        Assert.assertTrue(BookieImpl.addMasterKeyCache(masterKey2, l4));

        Assert.assertFalse(BookieImpl.addMasterKeyCache(masterKey1, l1));
        Assert.assertFalse(BookieImpl.addMasterKeyCache(masterKey1, l2));
        Assert.assertFalse(BookieImpl.addMasterKeyCache(masterKey2, l3));
        Assert.assertFalse(BookieImpl.addMasterKeyCache(masterKey2, l4));

        // test BookieImpl.getKeyFromMasterKeyCache
        Assert.assertEquals(masterKey1, BookieImpl.getKeyFromMasterKeyCache(l1));
        Assert.assertEquals(masterKey1, BookieImpl.getKeyFromMasterKeyCache(l2));
        Assert.assertEquals(masterKey2, BookieImpl.getKeyFromMasterKeyCache(l3));
        Assert.assertEquals(masterKey2, BookieImpl.getKeyFromMasterKeyCache(l4));

        // test BookieImpl.removeLedgerIdFromMasterKeyCache
        BookieImpl.removeLedgerIdFromMasterKeyCache(l1);
        BookieImpl.removeLedgerIdFromMasterKeyCache(l2);
        BookieImpl.removeLedgerIdFromMasterKeyCache(l3);
        BookieImpl.removeLedgerIdFromMasterKeyCache(l4);

        Assert.assertNull(BookieImpl.getKeyFromMasterKeyCache(l1));
        Assert.assertNull(BookieImpl.getKeyFromMasterKeyCache(l2));
        Assert.assertNull(BookieImpl.getKeyFromMasterKeyCache(l3));
        Assert.assertNull(BookieImpl.getKeyFromMasterKeyCache(l4));
    }
}