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
package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.junit.Test;

/**
 * Test utility methods from bookie request processor.
 */
public class TestBookieRequestProcessor {
    @Test
    public void testFlagsV3() {
        ReadRequest read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.FENCE_LEDGER).build();
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK).build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertTrue(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        read = ReadRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .build();
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.FENCE_LEDGER));
        assertFalse(RequestUtils.hasFlag(read, ReadRequest.Flag.ENTRY_PIGGYBACK));

        AddRequest add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertFalse(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));

        add = AddRequest.newBuilder()
            .setLedgerId(10).setEntryId(1)
            .setFlag(AddRequest.Flag.RECOVERY_ADD)
            .setMasterKey(ByteString.EMPTY)
            .setBody(ByteString.EMPTY)
            .build();
        assertTrue(RequestUtils.hasFlag(add, AddRequest.Flag.RECOVERY_ADD));
    }
}
