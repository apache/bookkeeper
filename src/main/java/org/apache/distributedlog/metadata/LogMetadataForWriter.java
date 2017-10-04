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
package org.apache.distributedlog.metadata;

import java.net.URI;
import org.apache.bookkeeper.versioning.Versioned;


/**
 * Log Metadata for writer.
 */
public class LogMetadataForWriter extends LogMetadata {

    private final Versioned<byte[]> maxLSSNData;
    private final Versioned<byte[]> maxTxIdData;
    private final Versioned<byte[]> allocationData;

    /**
     * metadata representation of a log.
     *
     * @param uri           namespace to store the log
     * @param logName       name of the log
     * @param logIdentifier identifier of the log
     */
    public LogMetadataForWriter(URI uri,
                                String logName,
                                String logIdentifier,
                                Versioned<byte[]> maxLSSNData,
                                Versioned<byte[]> maxTxIdData,
                                Versioned<byte[]> allocationData) {
        super(uri, logName, logIdentifier);
        this.maxLSSNData = maxLSSNData;
        this.maxTxIdData = maxTxIdData;
        this.allocationData = allocationData;
    }

    public Versioned<byte[]> getMaxLSSNData() {
        return maxLSSNData;
    }

    public Versioned<byte[]> getMaxTxIdData() {
        return maxTxIdData;
    }

    public Versioned<byte[]> getAllocationData() {
        return allocationData;
    }

}
