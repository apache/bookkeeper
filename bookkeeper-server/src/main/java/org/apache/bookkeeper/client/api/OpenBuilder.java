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

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * Builder-style interface to open exiting ledgers.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface OpenBuilder extends OpBuilder<ReadHandle> {

    /**
     * Set the id of the ledger to be opened.
     *
     * @param ledgerId
     *
     * @return the builder itself
     */
    OpenBuilder withLedgerId(long ledgerId);

    /**
     * Define to open the ledger in recovery mode or in readonly mode. In recovery mode the ledger will be fenced and
     * the writer of the ledger will be prevented from issuing other writes to the ledger. It defaults to 'false'
     *
     * @param recovery recovery mode
     *
     * @return the builder itself
     */
    OpenBuilder withRecovery(boolean recovery);

    /**
     * Sets the password to be used to open the ledger. It defauls to an empty password
     *
     * @param password the password to unlock the ledger
     *
     * @return the builder itself
     */
    OpenBuilder withPassword(byte[] password);

    /**
     * Sets the expected digest type used to check the contents of the ledger. It defaults to {@link DigestType#CRC32}.
     * If {@link ClientConfiguration#setEnableDigestTypeAutodetection(boolean) } is set to true this value is ignored
     * and the digest type is read directly from metadata
     *
     * @param digestType the type of digest
     *
     * @return the builder itself
     */
    OpenBuilder withDigestType(DigestType digestType);

}
