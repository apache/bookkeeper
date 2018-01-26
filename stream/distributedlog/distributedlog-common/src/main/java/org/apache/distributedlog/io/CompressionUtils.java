/**
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
package org.apache.distributedlog.io;

/**
 * Utils for compression related operations.
 */
public class CompressionUtils {

    public static final String LZ4 = "lz4";
    public static final String NONE = "none";

    /**
     * Get a cached compression codec instance for the specified type.
     * @param type compression codec type
     * @return compression codec instance
     */
    public static CompressionCodec getCompressionCodec(CompressionCodec.Type type) {
        if (type == CompressionCodec.Type.LZ4) {
            return LZ4CompressionCodec.of();
        }
        // No Compression
        return IdentityCompressionCodec.of();
    }

    /**
     * Compression type value from string.
     * @param compressionString compression codec presentation in string
     * @return compression codec type
     */
    public static CompressionCodec.Type stringToType(String compressionString) {
        if (compressionString.equals(LZ4)) {
            return CompressionCodec.Type.LZ4;
        } else if (compressionString.equals(NONE)) {
            return CompressionCodec.Type.NONE;
        } else {
            return CompressionCodec.Type.UNKNOWN;
        }
    }
}
