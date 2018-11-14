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
package org.apache.bookkeeper.common.collections;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to use "Thread.onSpinWait()" when available.
 */
@UtilityClass
@Slf4j
public class BusyWait {

    /**
     * If available (Java 9+), use intrinsic {@link Thread#onSpinWait} which will
     * reduce CPU consumption during the wait, otherwise fallback to regular
     * spinning.
     */
    public static void onSpinWait() {
        if (ON_SPIN_WAIT != null) {
            try {
                ON_SPIN_WAIT.invokeExact();
            } catch (Throwable t) {
                // Ignore
            }
        }
    }

    private static final MethodHandle ON_SPIN_WAIT;

    static {
        MethodHandle handle = null;
        try {
            handle = MethodHandles.lookup().findStatic(Thread.class, "onSpinWait", MethodType.methodType(void.class));
        } catch (Throwable t) {
            // Ignore
            if (log.isDebugEnabled()) {
                log.debug("Unable to use 'onSpinWait' from JVM", t);
            }
        }

        ON_SPIN_WAIT = handle;
    }
}
