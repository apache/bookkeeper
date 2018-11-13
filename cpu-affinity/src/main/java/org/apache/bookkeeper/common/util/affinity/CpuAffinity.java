/**
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

package org.apache.bookkeeper.common.util.affinity;

import lombok.experimental.UtilityClass;

import org.apache.bookkeeper.common.util.affinity.impl.CpuAffinityImpl;

/**
 * Utilities for enabling thread to CPU affinity.
 */
@UtilityClass
public class CpuAffinity {

    /**
     * Acquire ownership of one CPU core for the current thread.
     *
     * <p>Notes:
     *
     * <ol>
     * <li>This method will only consider CPUs that are "isolated" by the OS. Eg: boot the kernel with
     * <code>isolcpus=2,3,6,7</code> parameter
     * <li>
     * <li>This method will disable hyper-threading on the owned core
     * <li>Once a thread successfully acquires a CPU, ownership will be retained, even if the thread exits, for as long
     * as the JVM process is alive.
     * </ol>
     */
    public static void acquireCore() {
        CpuAffinityImpl.acquireCore();
    }
}
