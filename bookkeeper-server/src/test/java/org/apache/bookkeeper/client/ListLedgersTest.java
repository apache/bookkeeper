/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.Test;

/**
 * Test ListLedgers.
 */
public class ListLedgersTest extends BookKeeperClusterTestCase {

    private final DigestType digestType;

    public ListLedgersTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test
    void listLedgers()
        throws Exception {
        int numOfLedgers = 10;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);
        for (int i = 0; i < numOfLedgers; i++) {
            bkc.createLedger(digestType, "testPasswd".
                getBytes()).close();
        }

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
            getZooKeeperConnectString());
        Iterable<Long> iterable = admin.listLedgers();

        int counter = 0;
        for (Long lId : iterable) {
            counter++;
        }

        assertEquals(counter, numOfLedgers, "Wrong number of ledgers: " + numOfLedgers);
    }

    @Test
    void emptyList()
        throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
            getZooKeeperConnectString());
        Iterable<Long> iterable = admin.listLedgers();

        assertFalse(iterable.iterator().hasNext(), "There should be no ledger");
    }

    @Test
    void removeNotSupported()
        throws Exception {
        int numOfLedgers = 1;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        BookKeeper bkc = new BookKeeper(conf);
        for (int i = 0; i < numOfLedgers; i++) {
            bkc.createLedger(digestType, "testPasswd".
                getBytes()).close();
        }

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
            getZooKeeperConnectString());
        Iterator<Long> iterator = admin.listLedgers().iterator();
        iterator.next();
        try {
            iterator.remove();
        } catch (UnsupportedOperationException e) {
            // This exception is expected
            return;
        }

        fail("Remove is not supported, we shouln't have reached this point");

    }

}
