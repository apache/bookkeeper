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

public class WriteWaterMark {
    private static final int DEFAULT_LOW_WATER_MARK = 1;
    private static final int DEFAULT_HIGH_WATER_MARK = 1;

    private final int low;
    private final int high;

    public WriteWaterMark(int low, int high) {
        this.low = low;
        this.high = high;
    }

    public WriteWaterMark() {
        this.low = DEFAULT_LOW_WATER_MARK;
        this.high = DEFAULT_HIGH_WATER_MARK;
    }

    public int low() {
        return low;
    }

    public int high() {
        return high;
    }
}
