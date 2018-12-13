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

package org.apache.bookkeeper.bookie;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * This is the garbage collection thread status.
 * It includes what phase GarbageCollection (major/minor), gc counters, last gc time, etc.
 */
@Setter
@Getter
@Builder
public class GarbageCollectionStatus {
    // whether the GC thread is in force GC.
    private boolean forceCompacting;
    // whether the GC thread is in major compacting.
    private boolean majorCompacting;
    // whether the GC thread is in minor compacting.
    private boolean minorCompacting;

    private long lastMajorCompactionTime;
    private long lastMinorCompactionTime;
    private long majorCompactionCounter;
    private long minorCompactionCounter;
}
