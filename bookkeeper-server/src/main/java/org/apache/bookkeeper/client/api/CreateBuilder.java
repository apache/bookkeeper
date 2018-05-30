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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Builder-style interface to create new ledgers.
 *
 * @since 4.6
 * @see BookKeeper#newCreateLedgerOp()
 */
@Public
@Unstable
public interface CreateBuilder extends OpBuilder<WriteHandle> {

    /**
     * Set the number of bookies which will receive data for this ledger. It defaults to 3.
     *
     * @param ensembleSize the number of bookies
     *
     * @return the builder itself
     */
    CreateBuilder withEnsembleSize(int ensembleSize);

    /**
     * Set the number of bookies which receive every single entry.
     * In case of ensembleSize &gt; writeQuorumSize data will
     * be striped across a number of ensembleSize bookies. It defaults to 2.
     *
     * @param writeQuorumSize the replication factor for each entry
     *
     * @return the builder itself
     */
    CreateBuilder withWriteQuorumSize(int writeQuorumSize);

    /**
     * Set the number of acknowledgements to wait before considering a write to be completed with success. This value
     * can be less or equals to writeQuorumSize. It defaults to 2.
     *
     * @param ackQuorumSize the number of acknowledgements to wait for
     *
     * @return the builder itself
     */
    CreateBuilder withAckQuorumSize(int ackQuorumSize);

    /**
     * Set a password for the ledger. It defaults to empty password
     *
     * @param password the password
     *
     * @return the builder itself
     */
    CreateBuilder withPassword(byte[] password);

    /**
     * Set write flags. Write flags specify the behaviour of writes
     *
     * @param writeFlags the flags
     *
     * @return the builder itself
     */
    CreateBuilder withWriteFlags(EnumSet<WriteFlag> writeFlags);

    /**
     * Set write flags. Write flags specify the behaviour of writes
     *
     * @param writeFlags the flags
     *
     * @return the builder itself
     */
    default CreateBuilder withWriteFlags(WriteFlag ... writeFlags) {
        return withWriteFlags(EnumSet.copyOf(Arrays.asList(writeFlags)));
    }

    /**
     * Set a map a custom data to be attached to the ledger. The application is responsible for the semantics of these
     * data.
     *
     * @param customMetadata the ledger metadata
     *
     * @return the builder itself
     */
    CreateBuilder withCustomMetadata(Map<String, byte[]> customMetadata);

    /**
     * Set the Digest type used to guard data against corruption. It defaults to {@link DigestType#CRC32}
     *
     * @param digestType the type of digest
     *
     * @return the builder itself
     */
    CreateBuilder withDigestType(DigestType digestType);

    /**
     * Switch the ledger into 'Advanced' mode. A ledger used in Advanced mode will explicitly generate the sequence of
     * entry identifiers. Advanced ledgers can be created with a client side defined ledgerId
     *
     * @return a new {@link CreateAdvBuilder} builder
     */
    CreateAdvBuilder makeAdv();

}
