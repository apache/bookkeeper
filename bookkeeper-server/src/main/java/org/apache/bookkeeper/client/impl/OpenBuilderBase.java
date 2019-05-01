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

package org.apache.bookkeeper.client.impl;

import java.util.Arrays;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.OpenBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for open builders which does the mundane builder stuff.
 */
public abstract class OpenBuilderBase implements OpenBuilder {
    static final Logger LOG = LoggerFactory.getLogger(OpenBuilderBase.class);

    protected boolean recovery = false;
    protected long ledgerId = LedgerHandle.INVALID_LEDGER_ID;
    protected byte[] password;
    protected DigestType digestType = DigestType.CRC32;

    @Override
    public OpenBuilder withLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
        return this;
    }

    @Override
    public OpenBuilder withRecovery(boolean recovery) {
        this.recovery = recovery;
        return this;
    }

    @Override
    public OpenBuilder withPassword(byte[] password) {
        this.password = Arrays.copyOf(password, password.length);
        return this;
    }

    @Override
    public OpenBuilder withDigestType(DigestType digestType) {
        this.digestType = digestType;
        return this;
    }

    protected int validate() {
        if (ledgerId < 0) {
            LOG.error("invalid ledgerId {} < 0", ledgerId);
            return Code.NoSuchLedgerExistsOnMetadataServerException;
        }
        return Code.OK;
    }
}
