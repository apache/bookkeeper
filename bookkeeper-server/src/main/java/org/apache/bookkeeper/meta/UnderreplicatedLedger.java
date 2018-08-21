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
package org.apache.bookkeeper.meta;

import java.util.List;

/**
 * UnderReplicated ledger representation info.
 */
public class UnderreplicatedLedger {
    private final long ledgerId;
    private long ctime;
    private List<String> replicaList;
    public static final long UNASSIGNED_CTIME = -1L;

    protected UnderreplicatedLedger(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public long getCtime() {
        return ctime;
    }

    protected void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public List<String> getReplicaList() {
        return replicaList;
    }

    protected void setReplicaList(List<String> replicaList) {
        this.replicaList = replicaList;
    }

    public long getLedgerId() {
        return ledgerId;
    }
}
