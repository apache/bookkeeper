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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.Random;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Ledger Metadata serialization and deserialization.
 */
public class TestLedgerMetadataSerDe {
    // as used in 4.0.x & 4.1.x
    private static final String version1 =
        "Qm9va2llTWV0YWRhdGFGb3JtYXRWZXJzaW9uCTEKMgozCjAKMAkxOTIuMC4yLjE6MTIzNAkxOTIu"
        + "MC4yLjI6MTIzNAkxOTIuMC4yLjM6MTIzNAotMTAyCUNMT1NFRA==";

    // as used in 4.2.x & 4.3.x (text protobuf based metadata, password and digest introduced)
    private static final String version2 =
        "Qm9va2llTWV0YWRhdGFGb3JtYXRWZXJzaW9uCTIKcXVvcnVtU2l6ZTogMgplbnNlbWJsZVNpemU6I"
        + "DMKbGVuZ3RoOiAwCmxhc3RFbnRyeUlkOiAtMQpzdGF0ZTogSU5fUkVDT1ZFUlkKc2VnbWVudCB7"
        + "CiAgZW5zZW1ibGVNZW1iZXI6ICIxOTIuMC4yLjE6MTIzNCIKICBlbnNlbWJsZU1lbWJlcjogIjE"
        + "5Mi4wLjIuMjoxMjM0IgogIGVuc2VtYmxlTWVtYmVyOiAiMTkyLjAuMi4zOjEyMzQiCiAgZmlyc3"
        + "RFbnRyeUlkOiAwCn0KZGlnZXN0VHlwZTogQ1JDMzIKcGFzc3dvcmQ6ICJwYXNzd2QiCmFja1F1b"
        + "3J1bVNpemU6IDIK";

    // version 2 + ctime, as used in 4.4.x to 4.8.x (ctime is optional from 4.6.x onwards)
    private static final String version2ctime =
        "Qm9va2llTWV0YWRhdGFGb3JtYXRWZXJzaW9uCTIKcXVvcnVtU2l6ZTogMgplbnNlbWJsZVNpemU6I"
        + "DMKbGVuZ3RoOiAwCmxhc3RFbnRyeUlkOiAtMQpzdGF0ZTogSU5fUkVDT1ZFUlkKc2VnbWVudCB7"
        + "CiAgZW5zZW1ibGVNZW1iZXI6ICIxOTIuMC4yLjE6MTIzNCIKICBlbnNlbWJsZU1lbWJlcjogIjE"
        + "5Mi4wLjIuMjoxMjM0IgogIGVuc2VtYmxlTWVtYmVyOiAiMTkyLjAuMi4zOjEyMzQiCiAgZmlyc3"
        + "RFbnRyeUlkOiAwCn0KZGlnZXN0VHlwZTogQ1JDMzIKcGFzc3dvcmQ6ICJwYXNzd2QiCmFja1F1b"
        + "3J1bVNpemU6IDIKY3RpbWU6IDE1NDQwMDIzODMwNzUK";

    // version 3, since 4.9.x, protobuf binary format
    private static final String version3 =
        "Qm9va2llTWV0YWRhdGFGb3JtYXRWZXJzaW9uCTMKYAgCEAMYACD///////////8BKAEyMgoOMTkyL"
        + "jAuMi4xOjMxODEKDjE5Mi4wLjIuMjozMTgxCg4xOTIuMC4yLjM6MzE4MRAAOANCBmZvb2JhckgB"
        + "UP///////////wFgAA==";

    private static void testDecodeEncode(String encoded) throws Exception {
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        LedgerMetadata md = serDe.parseConfig(Base64.getDecoder().decode(encoded), Optional.empty());
        String reserialized = Base64.getEncoder().encodeToString(serDe.serialize(md));

        Assert.assertEquals(encoded, reserialized);
    }

    @Test
    public void testVersion1SerDe() throws Exception {
        testDecodeEncode(version1);
    }

    @Test
    public void testVersion2SerDe() throws Exception {
        testDecodeEncode(version2);
    }

    @Test
    public void testVersion2CtimeSerDe() throws Exception {
        testDecodeEncode(version2ctime);
    }

    @Test
    public void testVersion3SerDe() throws Exception {
        testDecodeEncode(version3);
    }

    @Test(expected = IOException.class)
    public void testJunkSerDe() throws Exception {
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        String junk = "";
        serDe.parseConfig(junk.getBytes(UTF_8), Optional.empty());
    }

    @Test(expected = IOException.class)
    public void testJunk2SerDe() throws Exception {
        byte[] randomBytes = new byte[1000];
        new Random().nextBytes(randomBytes);
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        serDe.parseConfig(randomBytes, Optional.empty());
    }

    @Test(expected = IOException.class)
    public void testJunkVersionSerDe() throws Exception {
        byte[] junkVersion = "BookieMetadataFormatVersion\tfoobar\nblahblah".getBytes(UTF_8);
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        serDe.parseConfig(junkVersion, Optional.empty());
    }

    @Test(expected = IOException.class)
    public void testVeryLongVersionSerDe() throws Exception {
        byte[] veryLongVersion = "BookieMetadataFormatVersion\t123456789123456789\nblahblah".getBytes(UTF_8);
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        serDe.parseConfig(veryLongVersion, Optional.empty());
    }

    @Test
    public void testPeggedToV2SerDe() throws Exception {
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
            .withPassword("foobar".getBytes(UTF_8)).withDigestType(DigestType.CRC32C)
            .newEnsembleEntry(0L, Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 3181),
                                                     new BookieSocketAddress("192.0.2.2", 3181),
                                                     new BookieSocketAddress("192.0.2.3", 3181)))
            .build();
        byte[] encoded = serDe.serialize(metadata);

        LedgerMetadata decoded = serDe.parseConfig(encoded, Optional.empty());
        Assert.assertEquals(2, decoded.getMetadataFormatVersion());
    }

    @Test
    public void testStoreSystemtimeAsLedgerCtimeEnabledWithVersion2()
            throws Exception {
        LedgerMetadata lm = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
            .withPassword("foobar".getBytes(UTF_8)).withDigestType(DigestType.CRC32C)
            .newEnsembleEntry(0L, Lists.newArrayList(
                                      new BookieSocketAddress("192.0.2.1", 1234),
                                      new BookieSocketAddress("192.0.2.2", 1234),
                                      new BookieSocketAddress("192.0.2.3", 1234)))
            .withCreationTime(123456L)
            .storingCreationTime(true)
            .build();
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        byte[] serialized = serDe.serialize(lm);
        LedgerMetadata deserialized = serDe.parseConfig(serialized, Optional.of(654321L));
        Assert.assertEquals(deserialized.getCtime(), 123456L);

        // give it another round
        LedgerMetadata deserialized2 = serDe.parseConfig(serDe.serialize(deserialized), Optional.of(98765L));
        Assert.assertEquals(deserialized2.getCtime(), 123456L);
    }

    @Test
    public void testStoreSystemtimeAsLedgerCtimeDisabledWithVersion2()
            throws Exception {
        LedgerMetadata lm = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
            .withPassword("foobar".getBytes(UTF_8)).withDigestType(DigestType.CRC32C)
            .newEnsembleEntry(0L, Lists.newArrayList(
                                      new BookieSocketAddress("192.0.2.1", 1234),
                                      new BookieSocketAddress("192.0.2.2", 1234),
                                      new BookieSocketAddress("192.0.2.3", 1234)))
            .build();

        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        byte[] serialized = serDe.serialize(lm);
        LedgerMetadata deserialized = serDe.parseConfig(serialized, Optional.of(654321L));
        Assert.assertEquals(deserialized.getCtime(), 654321L);

        // give it another round
        LedgerMetadata deserialized2 = serDe.parseConfig(serDe.serialize(deserialized), Optional.of(98765L));
        Assert.assertEquals(deserialized2.getCtime(), 98765L);
    }
}
