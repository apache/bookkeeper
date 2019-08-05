/*
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

package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
/**
 * Implements a ledger inside a bookie. In particular, it implements operations
 * to write entries to a ledger and read entries from a ledger.
 */
public class LedgerDescriptorReadOnlyImpl extends LedgerDescriptorImpl {
    LedgerDescriptorReadOnlyImpl(long ledgerId, LedgerStorage storage) {
        super(null, ledgerId, storage);
    }

    @Override
    boolean setFenced() throws IOException {
        assert false;
        throw new IOException("Invalid action on read only descriptor");
    }

    @Override
    long addEntry(ByteBuf entry) throws IOException {
        assert false;
        throw new IOException("Invalid action on read only descriptor");
    }

    @Override
    void checkAccess(byte[] masterKey) throws BookieException, IOException {
        assert false;
        throw new IOException("Invalid action on read only descriptor");
    }
}
