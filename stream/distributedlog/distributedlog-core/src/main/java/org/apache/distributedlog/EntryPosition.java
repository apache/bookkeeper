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

/**
 * The position of an entry, identified by log segment sequence number and entry id.
 */
class EntryPosition {

    private long lssn;
    private long entryId;

    EntryPosition(long lssn, long entryId) {
        this.lssn = lssn;
        this.entryId = entryId;
    }

    public synchronized long getLogSegmentSequenceNumber() {
        return lssn;
    }

    public synchronized long getEntryId() {
        return entryId;
    }

    public synchronized boolean advance(long lssn, long entryId) {
        if (lssn == this.lssn) {
            if (entryId <= this.entryId) {
                return false;
            }
            this.entryId = entryId;
            return true;
        } else if (lssn > this.lssn) {
            this.lssn = lssn;
            this.entryId = entryId;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(lssn).append(", ").append(entryId).append(")");
        return sb.toString();
    }
}
