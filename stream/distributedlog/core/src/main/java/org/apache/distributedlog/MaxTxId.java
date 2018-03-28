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
package org.apache.distributedlog;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.util.DLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for storing and reading
 * the max seen txid in zookeeper.
 */
class MaxTxId {
    static final Logger LOG = LoggerFactory.getLogger(MaxTxId.class);

    private Version version;
    private long currentMax;

    MaxTxId(Versioned<byte[]> maxTxIdData) {
        if (null != maxTxIdData
                && null != maxTxIdData.getValue()
                && null != maxTxIdData.getVersion()) {
            this.version = maxTxIdData.getVersion();
            try {
                this.currentMax = DLUtils.deserializeTransactionId(maxTxIdData.getValue());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid txn id stored in {}", e);
                this.currentMax = DistributedLogConstants.INVALID_TXID;
            }
        } else {
            this.currentMax = DistributedLogConstants.INVALID_TXID;
            if (null != maxTxIdData && null != maxTxIdData.getVersion()) {
                this.version = maxTxIdData.getVersion();
            } else {
                throw new IllegalStateException("Invalid MaxTxId found - " + maxTxIdData);
            }
        }
    }

    synchronized void update(Version version, long txId) {
        if (version.compare(this.version) == Version.Occurred.AFTER) {
            this.version = version;
            this.currentMax = txId;
        }
    }

    synchronized long get() {
        return currentMax;
    }

    public synchronized Versioned<Long> getVersionedData(long txId) {
        return new Versioned<Long>(Math.max(txId, get()), version);
    }

}
