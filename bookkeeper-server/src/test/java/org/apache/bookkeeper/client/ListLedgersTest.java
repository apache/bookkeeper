/**
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

import java.util.Iterator;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BaseTestCase;

import org.apache.zookeeper.KeeperException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListLedgersTest extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(ListLedgersTest.class);

    DigestType digestType;

    public ListLedgersTest (DigestType digestType) {
        super(4);
        this.digestType = digestType;
    }

    @Test(timeout=60000)
    public void testListLedgers()
    throws Exception {
        int numOfLedgers = 10;

        ClientConfiguration conf = new ClientConfiguration()
        .setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        for (int i = 0; i < numOfLedgers ; i++) {
            bkc.createLedger(digestType, "testPasswd".
                    getBytes()).close();
        }

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
                getZooKeeperConnectString());
        Iterable<Long> iterable = admin.listLedgers();

        int counter = 0;
        for (Long lId: iterable) {
            counter++;
        }

        Assert.assertTrue("Wrong number of ledgers: " + numOfLedgers,
                counter == numOfLedgers);
    }

    @Test(timeout=60000)
    public void testEmptyList()
    throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
        .setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
                getZooKeeperConnectString());
        Iterable<Long> iterable = admin.listLedgers();

        LOG.info("Empty list assertion");
        Assert.assertFalse("There should be no ledger", iterable.iterator().hasNext());
    }

    @Test(timeout=60000)
    public void testRemoveNotSupported()
    throws Exception {
        int numOfLedgers = 1;

        ClientConfiguration conf = new ClientConfiguration()
        .setZkServers(zkUtil.getZooKeeperConnectString());

        BookKeeper bkc = new BookKeeper(conf);
        for (int i = 0; i < numOfLedgers ; i++) {
            bkc.createLedger(digestType, "testPasswd".
                    getBytes()).close();
        }

        BookKeeperAdmin admin = new BookKeeperAdmin(zkUtil.
                getZooKeeperConnectString());
        Iterator<Long> iterator = admin.listLedgers().iterator();
        iterator.next();
        try{
            iterator.remove();
        } catch (UnsupportedOperationException e) {
            // This exception is expected
            return;
        }

        Assert.fail("Remove is not supported, we shouln't have reached this point");

    }
}
