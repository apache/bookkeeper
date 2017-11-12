/*
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
 */

package org.apache.bookkeeper.client;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.junit.Test;

/**
 * Unit test for ledger metadata
 */
public class LedgerMetadataTest {

    @Test
    public void testStoreSystemtimeAsLedgerCtimeEnabled()
            throws Exception {
        byte[] passwd = "testPasswd".getBytes(UTF_8);

        LedgerMetadata lm = new LedgerMetadata(
            3,
            3,
            2,
            DigestType.CRC32,
            passwd,
            Collections.emptyMap(),
            true);
        LedgerMetadataFormat format = lm.buildProtoFormat();
        assertTrue(format.hasCtime());
    }

    @Test
    public void testStoreSystemtimeAsLedgerCtimeDisabled()
            throws Exception {
        byte[] passwd = "testPasswd".getBytes(UTF_8);

        LedgerMetadata lm = new LedgerMetadata(
            3,
            3,
            2,
            DigestType.CRC32,
            passwd,
            Collections.emptyMap(),
            false);
        LedgerMetadataFormat format = lm.buildProtoFormat();
        assertFalse(format.hasCtime());
    }

}
