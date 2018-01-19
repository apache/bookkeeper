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

package org.apache.bookkeeper.proto.checksum;

/**
 * DigestType for checksum.
 * There are 3 digest types that can be used for verification. The CRC32 is
 * cheap to compute but does not protect against byzantine bookies (i.e., a
 * bookie might report fake bytes and a matching CRC32). The MAC code is more
 * expensive to compute, but is protected by a password, i.e., a bookie can't
 * report fake bytes with a mathching MAC unless it knows the password.
 * The CRC32C, which use SSE processor instruction, has better performance than CRC32.
 */
public enum DigestType {
    MAC, CRC32, CRC32C;

    public static DigestType fromBookKeeperDigestType(org.apache.bookkeeper.client.BookKeeper.DigestType digestType) {
        switch (digestType) {
            case MAC:
                return DigestType.MAC;
            case CRC32:
                return DigestType.CRC32;
            case CRC32C:
                return DigestType.CRC32C;
            default:
                throw new IllegalArgumentException("Unable to convert digest type " + digestType);
        }
    }
}
