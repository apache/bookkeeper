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


/**
 * Utility class for storing and reading max ledger sequence number.
 */
class MaxLogSegmentSequenceNo {

    Version version;
    long maxSeqNo;

    MaxLogSegmentSequenceNo(Versioned<byte[]> logSegmentsData) {
        if (null != logSegmentsData
                && null != logSegmentsData.getValue()
                && null != logSegmentsData.getVersion()) {
            version = logSegmentsData.getVersion();
            try {
                maxSeqNo = DLUtils.deserializeLogSegmentSequenceNumber(logSegmentsData.getValue());
            } catch (NumberFormatException nfe) {
                maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            }
        } else {
            maxSeqNo = DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO;
            if (null != logSegmentsData && null != logSegmentsData.getVersion()) {
                version = logSegmentsData.getVersion();
            } else {
                throw new IllegalStateException("Invalid MaxLogSegmentSequenceNo found - " + logSegmentsData);
            }
        }
    }

    synchronized Version getVersion() {
        return version;
    }

    synchronized long getSequenceNumber() {
        return maxSeqNo;
    }

    synchronized MaxLogSegmentSequenceNo update(Version version, long logSegmentSeqNo) {
        if (version.compare(this.version) == Version.Occurred.AFTER) {
            this.version = version;
            this.maxSeqNo = logSegmentSeqNo;
        }
        return this;
    }

    public synchronized Versioned<Long> getVersionedData(long seqNo) {
        return new Versioned<Long>(seqNo, version);
    }

}
