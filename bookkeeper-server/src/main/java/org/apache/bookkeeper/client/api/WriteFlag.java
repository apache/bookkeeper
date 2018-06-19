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
package org.apache.bookkeeper.client.api;

import java.util.EnumSet;
import lombok.Getter;

/**
 * Flags to specify the behaviour of writes.
 */
@Getter
public enum WriteFlag {

    /**
     * Writes will be acknowledged after writing to the filesystem
     * but not yet been persisted to disks.
     *
     * @see ForceableHandle#force()
     */
    DEFERRED_SYNC(0x1 << 0);

    /**
     * No flag is set, use default behaviour.
     */
    public static final EnumSet<WriteFlag> NONE = EnumSet.noneOf(WriteFlag.class);

    private static final EnumSet<WriteFlag> ONLY_DEFERRED_SYNC = EnumSet.of(DEFERRED_SYNC);

    private final int value;

    WriteFlag(int value) {
        this.value = value;
    }

    /**
     * Converts a set of flags from a binary representation.
     *
     * @param flagValue the binary value
     * @return a set of flags
     */
    public static EnumSet<WriteFlag> getWriteFlags(int flagValue) {
        if ((flagValue & DEFERRED_SYNC.value) == DEFERRED_SYNC.value) {
            return ONLY_DEFERRED_SYNC;
        }
        return WriteFlag.NONE;
    }

    /**
     * Converts a set of flags from a binary representation.
     *
     * @param flags the flags
     * @return the binary representation
     */
    public static int getWriteFlagsValue(EnumSet<WriteFlag> flags) {
        int result = 0;
        for (WriteFlag flag : flags) {
            result |= flag.value;
        }
        return result;
    }
}
