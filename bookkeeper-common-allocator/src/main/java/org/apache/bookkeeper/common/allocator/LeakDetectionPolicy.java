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
package org.apache.bookkeeper.common.allocator;

import lombok.extern.slf4j.Slf4j;

/**
 * Define the policy for the Netty leak detector.
 */
@Slf4j
public enum LeakDetectionPolicy {

    /**
     * No leak detection and no overhead.
     */
    Disabled,

    /**
     * Instruments 1% of the allocated buffer to track for leaks.
     */
    Simple,

    /**
     * Instruments 1% of the allocated buffer to track for leaks, reporting
     * stack traces of places where the buffer was used.
     */
    Advanced,

    /**
     * Instruments 100% of the allocated buffer to track for leaks, reporting
     * stack traces of places where the buffer was used. Introduce very
     * significant overhead.
     */
    Paranoid;

    public static LeakDetectionPolicy parseLevel(String levelStr) {
        String trimmedLevelStr = levelStr.trim();
        for (LeakDetectionPolicy policy : values()) {
            if (trimmedLevelStr.equalsIgnoreCase(policy.name())) {
                return policy;
            }
        }
        log.warn("Parse leak detection policy level {} failed. Use the default level: {}", levelStr,
                LeakDetectionPolicy.Disabled.name());
        return LeakDetectionPolicy.Disabled;
    }
}
