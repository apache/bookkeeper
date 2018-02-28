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

import java.io.File;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.rocksdb.AbstractImmutableNativeReference;

/**
 * Utils for interacting with rocksdb classes.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RocksUtils {

    public static void close(AbstractImmutableNativeReference ref) {
        if (null == ref) {
            return;
        }
        ref.close();
    }

    public static boolean isSstFile(File file) {
        return file.getName().endsWith(".sst");
    }

    public static String getDestCheckpointsPath(String dbPrefix) {
        return String.format("%s/checkpoints", dbPrefix);
    }

    public static String getDestCheckpointPath(String dbPrefix, String checkpointId) {
        return String.format("%s/checkpoints/%s", dbPrefix, checkpointId);
    }

    public static String getDestCheckpointMetadataPath(String dbPrefix, String checkpointId) {
        return String.format("%s/checkpoints/%s/metadata", dbPrefix, checkpointId);
    }

    public static String getDestSstsPath(String dbPrefix) {
        return String.format("%s/ssts", dbPrefix);
    }

    public static String getDestSstPath(String dbPrefix, File file) {
        return String.format("%s/ssts/%s", dbPrefix, file.getName());
    }

    public static String getDestSstPath(String dbPrefix, String fileName) {
        return String.format("%s/ssts/%s", dbPrefix, fileName);
    }

    public static String getDestTempSstPath(String dbPrefix, String checkpointId, File file) {
        return String.format("%s/checkpoints/%s/%s", dbPrefix, checkpointId, file.getName());
    }

    public static String getDestPath(String dbPrefix,
                                     String checkpointId,
                                     File file) {
        return String.format("%s/checkpoints/%s/%s", dbPrefix, checkpointId, file.getName());
    }

}
