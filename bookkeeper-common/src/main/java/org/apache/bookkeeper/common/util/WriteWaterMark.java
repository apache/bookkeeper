/**
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

/**
 * {@link WriteWaterMark} is used to configure the max value and the min value of the memory usage.
 */
public class WriteWaterMark {
    private static final long DEFAULT_LOW_WATER_MARK = 1610612736L; // 1.5GB
    private static final long DEFAULT_HIGH_WATER_MARK = 2147483648L; // 2GB

    private final long low;
    private final long high;

    public WriteWaterMark(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public WriteWaterMark() {
        this.low = DEFAULT_LOW_WATER_MARK;
        this.high = DEFAULT_HIGH_WATER_MARK;
    }

    public long low() {
        return low;
    }

    public long high() {
        return high;
    }
}
