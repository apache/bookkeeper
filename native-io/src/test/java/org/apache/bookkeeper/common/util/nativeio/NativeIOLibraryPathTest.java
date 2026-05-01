/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class NativeIOLibraryPathTest {

    private void assertLibraryCandidates(String osName, String osArch, String... expectedCandidates) {
        List<String> expected = expectedCandidates.length == 0
                ? Collections.emptyList()
                : List.of(expectedCandidates);
        assertEquals(expected, NativeIOLibraryPath.libraryCandidates(osName, osArch));
    }

    @Test
    public void testLinuxAmd64() {
        assertLibraryCandidates("Linux", "amd64", "/lib/linux-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testLinuxX8664() {
        assertLibraryCandidates("Linux", "x86_64", "/lib/linux-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testLinuxAarch64() {
        assertLibraryCandidates("Linux", "aarch64", "/lib/linux-aarch64-gnu/libnative-io.so");
    }

    @Test
    public void testLinuxUnknownArch() {
        assertThrows(IllegalArgumentException.class, () -> assertLibraryCandidates("Linux", "riscv64"));
    }

    @Test
    public void testMacOsAmd64() {
        assertLibraryCandidates("MacOS", "amd64", "/lib/macos-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testMacAmd64() {
        assertLibraryCandidates("Mac", "amd64", "/lib/macos-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testMacOsWithSpaceAmd64() {
        assertLibraryCandidates("Mac OS", "amd64", "/lib/macos-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testWindowsAmd64() {
        assertLibraryCandidates("Windows", "amd64", "/lib/windows-x86_64-gnu/libnative-io.so");
    }

    @Test
    public void testExplicitPathProperty() {
        assertEquals("/tmp/libnative-io.so",
                NativeIOLibraryPath.configuredLibraryPath("/tmp/libnative-io.so", null));
    }

    @Test
    public void testExplicitPathEnv() {
        assertEquals("/tmp/from-env.so",
                NativeIOLibraryPath.configuredLibraryPath(null, "/tmp/from-env.so"));
    }

    @Test
    public void testPropertyTakesPrecedenceOverEnv() {
        assertEquals("/tmp/prop.so",
                NativeIOLibraryPath.configuredLibraryPath("/tmp/prop.so", "/tmp/env.so"));
    }

    @Test
    public void testBlankValuesReturnNull() {
        assertNull(NativeIOLibraryPath.configuredLibraryPath("  ", ""));
    }
}
