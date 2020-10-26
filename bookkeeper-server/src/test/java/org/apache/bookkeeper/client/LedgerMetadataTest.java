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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

/**
 * Unit test for ledger metadata.
 */
public class LedgerMetadataTest {

    private static final byte[] passwd = "testPasswd".getBytes(UTF_8);

    @Test
    public void testGetters() {
        List<BookieId> ensemble = Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.3", 1234).toBookieId());
        org.apache.bookkeeper.client.api.LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
            .withDigestType(DigestType.CRC32.toApiDigestType()).withPassword(passwd)
            .newEnsembleEntry(0L, ensemble)
            .build();

        assertEquals(3, metadata.getEnsembleSize());
        assertEquals(2, metadata.getWriteQuorumSize());
        assertEquals(1, metadata.getAckQuorumSize());
        assertEquals(org.apache.bookkeeper.client.api.DigestType.CRC32, metadata.getDigestType());
        assertEquals(Collections.emptyMap(), metadata.getCustomMetadata());
        assertEquals(-1L, metadata.getCtime());
        assertEquals(-1L, metadata.getLastEntryId());
        assertEquals(0, metadata.getLength());
        assertFalse(metadata.isClosed());
        assertEquals(1, metadata.getAllEnsembles().size());
        assertEquals(ensemble, metadata.getAllEnsembles().get(0L));
        assertEquals(ensemble, metadata.getEnsembleAt(99L));
    }

    @Test
    public void testToString() {
        List<BookieId> ensemble = Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.3", 1234).toBookieId());

        LedgerMetadata lm1 = LedgerMetadataBuilder.create()
            .withDigestType(DigestType.CRC32.toApiDigestType())
            .withPassword(passwd)
            .newEnsembleEntry(0L, ensemble)
            .build();

        assertTrue("toString should contain password value",
                   lm1.toString().contains(Base64.getEncoder().encodeToString(passwd)));
        assertTrue("toSafeString should not contain password value", lm1.toSafeString().contains("OMITTED"));
    }
}
