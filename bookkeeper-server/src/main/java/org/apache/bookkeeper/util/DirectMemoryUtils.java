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
package org.apache.bookkeeper.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DirectMemory Utility.
 */
public class DirectMemoryUtils {
    /**
     * Returns the max configured size of direct memory for the JVM process.
     *
     * <p>Direct memory can be specified with the flag <code>-XX:MaxDirectMemorySize=8G</code> on the command line.
     * If not specified, the default value will be set to the max size of the JVM heap.
     */
    public static long maxDirectMemory() {
        try {

            Class<?> vm = Class.forName("sun.misc.VM");
            Method maxDirectMemory = vm.getDeclaredMethod("maxDirectMemory");
            Object result = maxDirectMemory.invoke(null, (Object[]) null);

            checkNotNull(result);
            checkArgument(result instanceof Long);
            return (Long) result;
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to get maxDirectMemory size from sun.misc.VM, falling back to max heap size", e);
            }
            return Runtime.getRuntime().maxMemory();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(DirectMemoryUtils.class);
}
