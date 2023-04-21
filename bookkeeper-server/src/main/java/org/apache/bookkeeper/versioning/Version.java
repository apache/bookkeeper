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
package org.apache.bookkeeper.versioning;

/**
 * An interface that allows us to determine if a given version happened before or after another version.
 */
public interface Version {

    /**
     * Initial version.
     */
    Version NEW = new Version() {
        @Override
        public Occurred compare(Version v) {
            if (null == v) {
                throw new NullPointerException("Version is not allowed to be null.");
            }
            if (this == v) {
                return Occurred.CONCURRENTLY;
            }
            return Occurred.BEFORE;
        }
    };

    /**
     * Match any version.
     */
    Version ANY = v -> {
        if (null == v) {
            throw new NullPointerException("Version is not allowed to be null.");
        }
        return Occurred.CONCURRENTLY;
    };

    /**
     * Define the sequence of versions.
     */
    enum Occurred {
        BEFORE, AFTER, CONCURRENTLY
    }

    Occurred compare(Version v);
}
