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

import org.apache.commons.lang.StringUtils;

/**
 * Utils to check java specification.
 */
public final class Java {

    private Java() { }

    private static final Version VERSION = parseVersion(System.getProperty("java.specification.version"));

    // Package private for testing
    static Version parseVersion(String versionString) {
        final String[] parts = StringUtils.split(versionString, '.');
        int majorVersion = Integer.parseInt(parts[0]);
        int minorVersion;
        if (parts.length > 1) {
            minorVersion = Integer.parseInt(parts[1]);
        } else {
            minorVersion = 0;
        }
        return new Version(majorVersion, minorVersion);
    }

    // Having these as static final provides the best opportunity for compilar optimization
    public static final boolean IS_JAVA9_COMPATIBLE = VERSION.isJava9Compatible();
    public static final boolean IS_JAVA8_COMPATIBLE = VERSION.isJava8Compatible();

    public static boolean isIbmJdk() {
        return System.getProperty("java.vendor").contains("IBM");
    }

    // Package private for testing
    static class Version {
        public final int majorVersion;
        public final int minorVersion;

        private Version(int majorVersion, int minorVersion) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @Override
        public String toString() {
            return "Version(majorVersion=" + majorVersion
                + ", minorVersion=" + minorVersion + ")";
        }

        // Package private for testing
        boolean isJava9Compatible() {
            return majorVersion >= 9;
        }

        // Package private for testing
        boolean isJava8Compatible() {
            return majorVersion > 1 || (majorVersion == 1 && minorVersion >= 8);
        }
    }

}
