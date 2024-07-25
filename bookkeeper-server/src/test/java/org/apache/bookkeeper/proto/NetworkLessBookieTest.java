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

package org.apache.bookkeeper.proto;

import static org.junit.Assert.fail;

import io.netty.channel.Channel;
import io.netty.channel.local.LocalChannel;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Tests of the main BookKeeper client using networkless communication.
 */
public class NetworkLessBookieTest extends BookKeeperClusterTestCase {

    public NetworkLessBookieTest() {
        super(1);
        baseConf.setDisableServerSocketBind(true);
        baseConf.setEnableLocalTransport(true);
    }

    @Test
    public void testUseLocalBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setZkTimeout(20000);

        try (BookKeeper bkc = new BookKeeper(conf)) {
            try (LedgerHandle h = bkc.createLedger(1, 1, DigestType.CRC32, "testPasswd".getBytes())) {
                h.addEntry("test".getBytes());
            }
        }

        for (int i = 0; i < bookieCount(); i++) {
            for (Channel channel : serverByIndex(i).nettyServer.allChannels) {
                if (!(channel instanceof LocalChannel)) {
                    fail();
                }
            }
        }
    }
}
