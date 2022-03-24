/*
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
 */

package org.apache.bookkeeper.bookie.datainteg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.MockRegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the DataIntegrityCookieValidation implementation of CookieValidation.
 */
@SuppressWarnings("deprecation")
public class CookieValidationTest {
    private static Logger log = LoggerFactory.getLogger(CookieValidationTest.class);
    final TmpDirs tmpDirs = new TmpDirs();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }

    private File initializedDir() throws Exception {
        File dir = tmpDirs.createNew("cookie", "validation");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(dir));
        return dir;
    }

    private static ServerConfiguration serverConf(boolean stampMissingCookies) {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setDataIntegrityStampMissingCookiesEnabled(stampMissingCookies);
        conf.setAdvertisedAddress("foobar");
        return conf;
    }

    private Versioned<byte[]> genCookie(ServerConfiguration conf) throws UnknownHostException {
        return new Versioned<>(Cookie.generateCookie(conf).build().toString()
                .getBytes(StandardCharsets.UTF_8), Version.NEW);
    }

    @Test
    public void testNoZkCookieAndEmptyDirsStampsNewCookie() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());

        ServerConfiguration conf = serverConf(false);
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockRegistrationManager regManager = new MockRegistrationManager();
        DataIntegrityCookieValidation v = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v.checkCookies(dirs);

        byte[] cookieBytes = regManager.readCookie(bookieId).getValue();
        assertThat(cookieBytes, notNullValue());
        assertThat(cookieBytes.length, greaterThan(0));

        Cookie regManagerCookie = Cookie.parseFromBytes(cookieBytes);

        for (File d : dirs) {
            assertThat(Cookie.readFromDirectory(d), equalTo(regManagerCookie));
        }
    }

    @Test(expected = BookieException.InvalidCookieException.class)
    public void testZkCookieAndEmptyDirsRaisesErrorWithoutMissingCookieStamping() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                initializedDir());

        ServerConfiguration conf = serverConf(false);
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockRegistrationManager regManager = new MockRegistrationManager();
        regManager.writeCookie(bookieId, genCookie(conf));
        DataIntegrityCookieValidation v = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v.checkCookies(dirs);
    }

    @Test
    public void testZkCookieAndEmptyDirsStampsNewCookieWithMissingCookieStamping() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                initializedDir());

        ServerConfiguration conf = serverConf(true);
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockRegistrationManager regManager = new MockRegistrationManager();
        regManager.writeCookie(bookieId, genCookie(conf));
        DataIntegrityCookieValidation v = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v.checkCookies(dirs);

        byte[] cookieBytes = regManager.readCookie(bookieId).getValue();
        assertThat(cookieBytes, notNullValue());
        assertThat(cookieBytes.length, greaterThan(0));

        Cookie regManagerCookie = Cookie.parseFromBytes(cookieBytes);

        for (File d : dirs) {
            assertThat(Cookie.readFromDirectory(d), equalTo(regManagerCookie));
        }
    }

    @Test(expected = BookieException.InvalidCookieException.class)
    public void testMissingZKCookieRaisesError() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                initializedDir());

        ServerConfiguration conf = serverConf(true);

        MockRegistrationManager regManager = new MockRegistrationManager();
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v1.checkCookies(dirs);

        MockRegistrationManager blankRegManager = new MockRegistrationManager();
        DataIntegrityCookieValidation v2 = new DataIntegrityCookieValidation(
                conf, blankRegManager, new MockDataIntegrityCheck());
        v2.checkCookies(dirs);
    }

    @Test
    public void testMatchingCookiesTakesNoAction() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());

        ServerConfiguration conf = serverConf(true);

        MockRegistrationManager regManager = new MockRegistrationManager();
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v1.checkCookies(dirs); // stamp original cookies

        DataIntegrityCookieValidation v2 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v2.checkCookies(dirs); // should find cookies and return successfully
    }

    @Test
    public void testEmptyDirectoryTriggersIntegrityCheck() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());
        ServerConfiguration conf = serverConf(true);

        MockRegistrationManager regManager = new MockRegistrationManager();
        MockDataIntegrityCheck dataIntegCheck = spy(new MockDataIntegrityCheck());
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, dataIntegCheck);
        v1.checkCookies(dirs); // stamp original cookies
        verify(dataIntegCheck, times(0)).runPreBootCheck("INVALID_COOKIE");

        dirs.add(initializedDir());
        v1.checkCookies(dirs); // stamp original cookies
        verify(dataIntegCheck, times(1)).runPreBootCheck("INVALID_COOKIE");

        v1.checkCookies(dirs); // stamp original cookies
        verify(dataIntegCheck, times(1)).runPreBootCheck("INVALID_COOKIE");
    }

    @Test
    public void testErrorInIntegrityCheckPreventsStamping() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());

        ServerConfiguration conf = serverConf(true);

        MockRegistrationManager regManager = spy(new MockRegistrationManager());
        MockDataIntegrityCheck dataIntegCheck = spy(new MockDataIntegrityCheck() {
                @Override
                public CompletableFuture<Void> runPreBootCheck(String reason) {
                    return FutureUtils.exception(new BookieException.InvalidCookieException("blah"));
                }
            });

        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, dataIntegCheck);

        v1.checkCookies(dirs); // stamp original cookies
        verify(dataIntegCheck, times(0)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(1)).writeCookie(anyObject(), anyObject());

        // add a directory to trigger data integrity check
        dirs.add(initializedDir());
        try {
            v1.checkCookies(dirs); // stamp original cookies
            Assert.fail("failure of data integrity should fail cookie check");
        } catch (BookieException.InvalidCookieException e) {
            // expected
        }
        verify(dataIntegCheck, times(1)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(1)).writeCookie(anyObject(), anyObject());

        // running the check again should run data integrity again, as stamping didn't happen
        try {
            v1.checkCookies(dirs); // stamp original cookies
            Assert.fail("failure of data integrity should fail cookie check");
        } catch (BookieException.InvalidCookieException e) {
            // expected
        }
        verify(dataIntegCheck, times(2)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(1)).writeCookie(anyObject(), anyObject());
    }

    @Test
    public void testChangingBookieIdRaisesError() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());
        ServerConfiguration conf = serverConf(true);
        MockRegistrationManager regManager = new MockRegistrationManager();
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v1.checkCookies(dirs); // stamp original cookies

        conf.setAdvertisedAddress("barfoo");
        DataIntegrityCookieValidation v2 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        try {
            v2.checkCookies(dirs); // should fail as cookie not found in ZK, but exists in dirs
            Assert.fail("Check shouldn't have succeeded with new bookieId");
        } catch (BookieException.InvalidCookieException ice) {
            // expected
        }

        conf.setAdvertisedAddress("foobar");
        DataIntegrityCookieValidation v3 = new DataIntegrityCookieValidation(
                conf, regManager, new MockDataIntegrityCheck());
        v3.checkCookies(dirs); // should succeed as the cookie is same as before
    }

    @Test
    public void testMismatchLocalCookie() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());

        ServerConfiguration conf = serverConf(true);

        MockDataIntegrityCheck dataIntegCheck = spy(new MockDataIntegrityCheck());
        MockRegistrationManager regManager = spy(new MockRegistrationManager());
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, dataIntegCheck);
        v1.checkCookies(dirs); // stamp original cookies

        verify(dataIntegCheck, times(0)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(1)).writeCookie(anyObject(), anyObject());

        Cookie current = Cookie.readFromDirectory(dirs.get(0));
        Cookie mismatch = Cookie.newBuilder(current).setBookieId("mismatch:3181").build();
        mismatch.writeToDirectory(dirs.get(0));
        assertThat(current, not(Cookie.readFromDirectory(dirs.get(0))));

        v1.checkCookies(dirs);
        verify(dataIntegCheck, times(1)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(2)).writeCookie(anyObject(), anyObject());

        Cookie afterCheck = Cookie.readFromDirectory(dirs.get(0));
        assertThat(afterCheck, equalTo(current));
    }

    @Test(expected = BookieException.InvalidCookieException.class)
    public void testCorruptLocalCookie() throws Exception {
        List<File> dirs = Lists.newArrayList(initializedDir(),
                                             initializedDir());

        ServerConfiguration conf = serverConf(true);

        MockDataIntegrityCheck dataIntegCheck = spy(new MockDataIntegrityCheck());
        MockRegistrationManager regManager = spy(new MockRegistrationManager());
        DataIntegrityCookieValidation v1 = new DataIntegrityCookieValidation(
                conf, regManager, dataIntegCheck);
        v1.checkCookies(dirs); // stamp original cookies

        verify(dataIntegCheck, times(0)).runPreBootCheck("INVALID_COOKIE");
        verify(regManager, times(1)).writeCookie(anyObject(), anyObject());

        File cookieFile = new File(dirs.get(0), BookKeeperConstants.VERSION_FILENAME);
        try (FileOutputStream out = new FileOutputStream(cookieFile)) {
            out.write(0xdeadbeef);
        }
        v1.checkCookies(dirs); // should throw
    }
}

