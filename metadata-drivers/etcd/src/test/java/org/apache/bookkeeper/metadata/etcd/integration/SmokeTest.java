/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.metadata.etcd.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdBKClusterTestBase;
import org.junit.Test;

/**
 * Smoke testing etcd metadata drives.
 */
@Slf4j
public class SmokeTest extends EtcdBKClusterTestBase {

    private static final byte[] PASSWD = "smoketest".getBytes(UTF_8);

    private static void readEntries(BookKeeper bk,
                                    long ledgerId,
                                    int numExpectedEntries) throws Exception {
        try (ReadHandle readlh = result(bk.newOpenLedgerOp()
            .withLedgerId(ledgerId)
            .withDigestType(DigestType.CRC32C)
            .withPassword(PASSWD)
            .execute()
        )) {
            long lac = readlh.getLastAddConfirmed();
            AtomicInteger idx = new AtomicInteger(0);
            try (LedgerEntries entries = readlh.read(0, lac)) {
                entries.forEach(e -> assertEquals(
                    String.format("entry-%03d", idx.getAndIncrement()),
                    new String(e.getEntryBytes(), UTF_8)));
            }
            assertEquals(idx.get(), numExpectedEntries);
        }
    }

    @Test
    public void testReadWrite() throws Exception {
        int numEntries = 100;
        try (BookKeeper bk = BookKeeper.newBuilder(conf).build()) {
            long ledgerId;
            try (WriteHandle wh = result(bk.newCreateLedgerOp()
                 .withDigestType(DigestType.CRC32C)
                 .withPassword(PASSWD)
                 .execute())) {
                ledgerId = wh.getId();
                log.info("Successfully created ledger {} to append entries.", ledgerId);
                for (int i = 0; i < numEntries; i++) {
                    wh.append(String.format("entry-%03d", i).getBytes(UTF_8));
                }
            }
            log.info("Opening ledger {} to read entries ...", ledgerId);
            readEntries(bk, ledgerId, numEntries);
            log.info("Successfully read {} entries from ledger {}", numEntries, ledgerId);
        }
    }

    @Test
    public void testReadWriteAdv() throws Exception {
        final int numEntries = 100;
        try (BookKeeper bk = BookKeeper.newBuilder(conf).build()) {
            long ledgerId;
            try (WriteAdvHandle wah = result(bk.newCreateLedgerOp()
                .withDigestType(DigestType.CRC32C)
                .withPassword(PASSWD)
                .makeAdv()
                .execute())) {
                ledgerId = wah.getId();
                log.info("Successfully created adv ledger {} to append entries.", ledgerId);
                for (int i = 0; i < numEntries; i++) {
                    wah.write(i, String.format("entry-%03d", i).getBytes(UTF_8));
                }
            }
            log.info("Opening adv ledger {} to read entries ...", ledgerId);
            readEntries(bk, ledgerId, numEntries);
            log.info("Successfully read {} entries from adv ledger {}", numEntries, ledgerId);
        }
    }



}
