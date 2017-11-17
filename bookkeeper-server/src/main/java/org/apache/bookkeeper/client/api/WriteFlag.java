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

import com.google.common.base.Preconditions;
import java.util.EnumSet;

/**
 * Flags to specify the behaviour of writes
 */
public enum WriteFlag {
    DEFERRED_FORCE(1);

    private final int value;

    WriteFlag(int value) {
        this.value = value;
    }

    /**
     * Converts a set of flags from a binary representation
     *
     * @param flagValue the binary value
     * @return a set of flags
     */
    public static EnumSet<WriteFlag> getWriteFlags(int flagValue) {
        Preconditions.checkArgument(flagValue >= 0 && flagValue <= 1);
        if ((flagValue & 1) == 1) {
            return EnumSet.of(DEFERRED_FORCE);
        }
        return EnumSet.noneOf(WriteFlag.class);
    }

    /**
     * Converts a set of flags from a binary representation
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
