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

package org.apache.bookkeeper.common.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;
import org.junit.Test;

/**
 * Unit test {@link ServiceURI}.
 */
public class ServiceURITest {

    private static void assertServiceUri(
        String serviceUri,
        String expectedServiceName,
        String[] expectedServiceInfo,
        String expectedServiceUser,
        String[] expectedServiceHosts,
        String expectedServicePath) {

        ServiceURI serviceURI = ServiceURI.create(serviceUri);

        assertEquals(expectedServiceName, serviceURI.getServiceName());
        assertArrayEquals(expectedServiceInfo, serviceURI.getServiceInfos());
        assertEquals(expectedServiceUser, serviceURI.getServiceUser());
        assertArrayEquals(expectedServiceHosts, serviceURI.getServiceHosts());
        assertEquals(expectedServicePath, serviceURI.getServicePath());
    }

    @Test
    public void testInvalidServiceUris() {
        String[] uris = new String[] {
            "://localhost:2181/path/to/namespace",          // missing scheme
            "bk:///path/to/namespace",                      // missing authority
            "bk://localhost:2181:3181/path/to/namespace",   // invalid hostname pair
            "bk://localhost:xyz/path/to/namespace",         // invalid port
            "bk://localhost:-2181/path/to/namespace",       // negative port
        };

        for (String uri : uris) {
            testInvalidServiceUri(uri);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullServiceUriString() {
        ServiceURI.create((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullServiceUriInstance() {
        ServiceURI.create((URI) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyServiceUriString() {
        ServiceURI.create("");
    }

    private void testInvalidServiceUri(String serviceUri) {
        try {
            ServiceURI.create(serviceUri);
            fail("Should fail to parse service uri : " + serviceUri);
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testMissingServiceName() {
        String serviceUri = "//localhost:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            null, new String[0], null, new String[] { "localhost:2181" }, "/path/to/namespace");
    }

    @Test
    public void testEmptyPath() {
        String serviceUri = "bk://localhost:2181";
        assertServiceUri(
            serviceUri,
            "bk", new String[0], null, new String[] { "localhost:2181" }, "");
    }

    @Test
    public void testRootPath() {
        String serviceUri = "bk://localhost:2181/";
        assertServiceUri(
            serviceUri,
            "bk", new String[0], null, new String[] { "localhost:2181" }, "/");
    }

    @Test
    public void testUserInfo() {
        String serviceUri = "bk://bookkeeper@localhost:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            "bookkeeper",
            new String[] { "localhost:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsSemiColon() {
        String serviceUri = "bk://host1:2181;host2:2181;host3:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            null,
            new String[] { "host1:2181", "host2:2181", "host3:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsComma() {
        String serviceUri = "bk://host1:2181,host2:2181,host3:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            null,
            new String[] { "host1:2181", "host2:2181", "host3:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsWithoutPorts() {
        String serviceUri = "bk://host1,host2,host3/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            null,
            new String[] { "host1:4181", "host2:4181", "host3:4181" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsMixedPorts() {
        String serviceUri = "bk://host1:3181,host2,host3:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            null,
            new String[] { "host1:3181", "host2:4181", "host3:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsMixed() {
        String serviceUri = "bk://host1:2181,host2,host3:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            null,
            new String[] { "host1:2181", "host2:4181", "host3:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testUserInfoWithMultipleHosts() {
        String serviceUri = "bk://bookkeeper@host1:2181;host2:2181;host3:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[0],
            "bookkeeper",
            new String[] { "host1:2181", "host2:2181", "host3:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testServiceInfoPlus() {
        String serviceUri = "bk+ssl://host:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk",
            new String[] { "ssl" },
            null,
            new String[] { "host:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testServiceInfoMinus() {
        String serviceUri = "bk-ssl://host:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "bk-ssl",
            new String[0],
            null,
            new String[] { "host:2181" },
            "/path/to/namespace");
    }

    @Test
    public void testServiceInfoDlogMinus() {
        String serviceUri = "distributedlog-bk://host:2181/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "distributedlog",
            new String[] { "bk" },
            null,
            new String[] { "host:2181" },
            "/path/to/namespace");
    }

}
