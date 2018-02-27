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

package org.apache.bookkeeper.statelib.impl.rocksdb;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.rocksdb.ChecksumType;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;

/**
 * Define the constants used by rocksdb based on state store.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RocksConstants {

    public static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.LZ4_COMPRESSION;
    public static final CompactionStyle DEFAULT_COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
    public static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
    public static final long BLOCK_CACHE_SIZE = 64 * 1024 * 1024;
    public static final long BLOCK_SIZE = 4096L;
    public static final int MAX_WRITE_BUFFERS = 3;
    public static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.kNoChecksum;
    public static final InfoLogLevel DEFAULT_LOG_LEVEL = InfoLogLevel.INFO_LEVEL;
    public static final int DEFAULT_PARALLELISM = Math.max(Runtime.getRuntime().availableProcessors(), 2);

}
