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

package org.apache.bookkeeper.metadata.etcd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test Etcd based cookie management.
 */
public class EtcdCookieTest extends EtcdTestBase {

    @Rule
    public final TestName runtime = new TestName();

    private RegistrationManager regMgr;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        String scope = RandomStringUtils.randomAlphabetic(16);
        this.regMgr = new EtcdRegistrationManager(
            newEtcdClient(),
            scope
        );
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.regMgr.close();
        super.tearDown();
    }

    private static void assertCookieEquals(Versioned<byte[]> expected, Versioned<byte[]> actual) {
        assertEquals(Occurred.CONCURRENTLY, expected.getVersion().compare(actual.getVersion()));
        assertArrayEquals(expected.getValue(), actual.getValue());
    }

    @Test
    public void readWriteRemoveCookie() throws Exception {
        BookieId bookieId = BookieId.parse(runtime.getMethodName() + ":3181");

        // read the cookie doesn't exist
        try {
            regMgr.readCookie(bookieId);
            fail("Should fail reading cookie if cookie doesn't exist");
        } catch (CookieNotFoundException cnfe) {
            // expected
        }

        // create the cookie
        String cookieData = RandomStringUtils.randomAlphanumeric(1024);
        Versioned<byte[]> cookie = new Versioned<>(
            cookieData.getBytes(UTF_8), Version.NEW
        );
        regMgr.writeCookie(bookieId, cookie);

        // read the cookie
        Versioned<byte[]> readCookie = regMgr.readCookie(bookieId);
        assertEquals(cookieData, new String(readCookie.getValue(), UTF_8));

        // attempt to create the cookie again
        String newCookieData = RandomStringUtils.randomAlphabetic(512);
        Versioned<byte[]> newCookie = new Versioned<>(
            newCookieData.getBytes(UTF_8), Version.NEW
        );
        try {
            regMgr.writeCookie(bookieId, newCookie);
            fail("Should fail creating cookie if the cookie already exists");
        } catch (MetadataStoreException mse) {
            assertTrue(mse.getMessage().contains("Conflict on writing cookie"));
        }
        Versioned<byte[]> readCookie2 = regMgr.readCookie(bookieId);
        assertCookieEquals(readCookie, readCookie2);

        // attempt to update the cookie with a wrong version
        newCookie = new Versioned<>(
            newCookieData.getBytes(UTF_8), new LongVersion(Long.MAX_VALUE)
        );
        try {
            regMgr.writeCookie(bookieId, newCookie);
        } catch (MetadataStoreException mse) {
            assertTrue(mse.getMessage().contains("Conflict on writing cookie"));
        }
        readCookie2 = regMgr.readCookie(bookieId);
        assertCookieEquals(readCookie, readCookie2);

        // delete the cookie with a wrong version
        LongVersion badVersion = new LongVersion(Long.MAX_VALUE);
        try {
            regMgr.removeCookie(bookieId, badVersion);
            fail("Should fail to remove cookie with bad version");
        } catch (MetadataStoreException mse) {
            assertTrue(mse.getMessage().contains(
                "bad version '" + badVersion + "'"
            ));
        }
        readCookie2 = regMgr.readCookie(bookieId);
        assertCookieEquals(readCookie, readCookie2);

        // update the cookie with right version
        newCookie = new Versioned<>(
            newCookieData.getBytes(UTF_8), readCookie2.getVersion());
        regMgr.writeCookie(bookieId, newCookie);
        readCookie2 = regMgr.readCookie(bookieId);
        assertEquals(newCookieData, new String(readCookie2.getValue(), UTF_8));
        assertEquals(Occurred.AFTER, readCookie2.getVersion().compare(readCookie.getVersion()));

        // delete the cookie with right version
        regMgr.removeCookie(bookieId, readCookie2.getVersion());
        try {
            regMgr.readCookie(bookieId);
            fail("Should fail reading cookie if cookie doesn't exist");
        } catch (CookieNotFoundException cnfe) {
            // expected
        }

        // remove a cookie that doesn't exist
        try {
            regMgr.removeCookie(bookieId, readCookie2.getVersion());
            fail("Should fail removing cookie if cookie doesn't exist");
        } catch (CookieNotFoundException cnfe) {
            // expected
        }
    }

}
