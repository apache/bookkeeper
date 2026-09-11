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
 *
 */
package org.apache.bookkeeper.common.util.nativeio;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

/**
 * Resolves the path of the native-io shared library inside the JAR.
 *
 * <p>The {@code cargo-zigbuild} Maven profile embeds two variants:
 * <pre>
 *   lib/linux-x86_64-gnu/libnative-io.so   (glibc, amd64)
 *   lib/linux-aarch64-gnu/libnative-io.so  (glibc, arm64)
 * </pre>
 *
 * <p>An explicit override path can be set via the system property
 * {@code bookkeeper.native.io.library.path} or the environment variable
 * {@code BOOKKEEPER_NATIVE_IO_LIBRARY_PATH}.
 */
public class NativeIOLibraryPath {

    private static final String LIBRARY_PATH_ENV = "BOOKKEEPER_NATIVE_IO_LIBRARY_PATH";
    private static final String LIBRARY_PATH_PROPERTY = "bookkeeper.native.io.library.path";

    private NativeIOLibraryPath() {
    }

    /**
     * Returns an explicit path from system property / env, or {@code null}.
     */
    public static String configuredLibraryPath() {
        return configuredLibraryPath(
                System.getProperty(LIBRARY_PATH_PROPERTY),
                System.getenv(LIBRARY_PATH_ENV));
    }

    protected static String configuredLibraryPath(String propertyValue, String envValue) {
        return StringUtils.isNotBlank(propertyValue) ? propertyValue : StringUtils.stripToNull(envValue);
    }

    /**
     * Returns the ordered list of JAR-resource paths to try for the current
     * platform and architecture. The first path that successfully loads wins.
     */
    public static List<String> currentPlatformLibraryCandidates() {
        if (SystemUtils.IS_OS_LINUX) {
            return libraryCandidates(SystemUtils.OS_NAME, SystemUtils.OS_ARCH);
        } else {
            return Collections.emptyList();
        }
    }

    protected static List<String> libraryCandidates(String osName, String osArch) {
        List<String> paths = new ArrayList<>();
        String osNameTag = osNameTag(osName);
        String archTag = archTag(osArch);
        paths.add("/lib/" + osNameTag + "-" + archTag + "-gnu/libnative-io.so");
        return paths;
    }

    private static String osNameTag(String osName) {
        if (osName == null) {
            throw new IllegalArgumentException("osName is null");
        }

        String normalized = osName.toLowerCase(Locale.US);

        if (normalized.contains("mac") || normalized.contains("darwin")) {
            return "macos";
        }

        if (normalized.startsWith("win")
                || normalized.contains("mingw")
                || normalized.contains("msys")
                || normalized.contains("cygwin")) {
            return "windows";
        }

        if (normalized.contains("linux")) {
            return "linux";
        }

        throw new IllegalArgumentException("Unsupported OS: " + osName);
    }

    private static final Pattern NON_ALNUM = Pattern.compile("[^a-z0-9]");

    private static String archTag(String osArch) {
        if (osArch == null) {
            throw new IllegalArgumentException("osArch is null");
        }

        String arch = NON_ALNUM.matcher(osArch.toLowerCase(Locale.US)).replaceAll("");

        return switch (arch) {
            case "amd64", "x8664", "x64" -> "x86_64";
            case "aarch64", "arm64", "armv8", "armv8l" -> "aarch64";
            default -> throw new IllegalArgumentException("Unsupported arch: " + osArch);
        };
    }
}
