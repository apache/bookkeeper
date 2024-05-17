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
package org.apache.bookkeeper.client.api;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.TestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests about ExplicitLAC and {@link Handle} API.
 */
public class ExplicitLACWithWriteHandleAPITest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ExplicitLACWithWriteHandleAPITest.class);

    public ExplicitLACWithWriteHandleAPITest() {
        super(1);
    }

    @Test
    public void testUseExplicitLAC() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setExplictLacInterval(1000);
        try (BookKeeper bkc = BookKeeper
                .newBuilder(conf)
                .build();) {
            try (WriteHandle writer = bkc.newCreateLedgerOp()
                    .withAckQuorumSize(1)
                    .withEnsembleSize(1)
                    .withPassword(new byte[0])
                    .withWriteQuorumSize(1)
                    .execute()
                    .get();) {
                writer.append("foo".getBytes("utf-8"));
                writer.append("foo".getBytes("utf-8"));
                writer.append("foo".getBytes("utf-8"));
                long expectedLastAddConfirmed = writer.append("foo".getBytes("utf-8"));

                // since BK 4.12.0 the reader automatically uses ExplicitLAC
                try (ReadHandle r = bkc.newOpenLedgerOp()
                        .withRecovery(false)
                        .withPassword(new byte[0])
                        .withLedgerId(writer.getId())
                        .execute()
                        .get()) {
                    TestUtils.assertEventuallyTrue("ExplicitLAC did not ork", () -> {
                        try {
                            long value = r.readLastAddConfirmed();
                            LOG.info("current value " + value + " vs " + expectedLastAddConfirmed);
                            return value == expectedLastAddConfirmed;
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }, 30, TimeUnit.SECONDS);
                }

            }

        }
    }
}
