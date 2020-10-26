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

package org.apache.bookkeeper.statelib.impl;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Default a few constants used across implementation.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Constants {

    public static final String LEDGER_METADATA_APPLICATION_STREAM_STORAGE = "bk-stream-storage-service";
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] NULL_START_KEY = EMPTY_BYTE_ARRAY;
    public static final byte[] NULL_END_KEY = new byte[] { 0 };
    public static final long INVALID_REVISION = -1L;

    public static boolean isNullStartKey(byte[] key) {
        return null != key && key.length == 0;
    }

    public static boolean isNullEndKey(byte[] key) {
        return null != key && key.length == 1 && key[0] == 0;
    }

}
