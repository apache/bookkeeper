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
package org.apache.bookkeeper.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link Java} util class.
 */
public class JavaTest {

    private String vendor;

    @Before
    public void setup() {
        vendor = System.getProperty("java.vendor");
    }

    @After
    public void teardown() {
        System.getProperty("java.vendor", vendor);
    }

    @Test
    public void testIsIBMJdk() {
        System.setProperty("java.vendor", "Oracle Corporation");
        assertFalse(Java.isIbmJdk());
        System.setProperty("java.vendor", "IBM Corporation");
        assertTrue(Java.isIbmJdk());
    }

    @Test
    public void testLoadKerberosLoginModule() throws Exception {
        String clazz = Java.isIbmJdk()
            ? "com.ibm.security.auth.module.Krb5LoginModule"
            : "com.sun.security.auth.module.Krb5LoginModule";
        Class.forName(clazz);
        assertTrue("Should be able to load kerberos login module", true);
    }

    @Test
    public void testJavaVersion() {
        Java.Version v = Java.parseVersion("9");
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());
        assertTrue(v.isJava8Compatible());

        v = Java.parseVersion("9.0.1");
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());
        assertTrue(v.isJava8Compatible());

        v = Java.parseVersion("9.0.0.15");
        assertEquals(9, v.majorVersion);
        assertEquals(0, v.minorVersion);
        assertTrue(v.isJava9Compatible());
        assertTrue(v.isJava8Compatible());

        v = Java.parseVersion("9.1");
        assertEquals(9, v.majorVersion);
        assertEquals(1, v.minorVersion);
        assertTrue(v.isJava9Compatible());
        assertTrue(v.isJava8Compatible());

        v = Java.parseVersion("1.8.0_152");
        assertEquals(1, v.majorVersion);
        assertEquals(8, v.minorVersion);
        assertFalse(v.isJava9Compatible());
        assertTrue(v.isJava8Compatible());

        v = Java.parseVersion("1.7.0_80");
        assertEquals(1, v.majorVersion);
        assertEquals(7, v.minorVersion);
        assertFalse(v.isJava9Compatible());
        assertFalse(v.isJava8Compatible());
    }


}
