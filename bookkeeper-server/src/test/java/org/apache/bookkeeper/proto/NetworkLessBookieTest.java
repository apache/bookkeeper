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

import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BookKeeper;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BaseTestCase;
import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.local.LocalChannel;

/**
 * Tests of the main BookKeeper client using networkless comunication
 */
public class NetworkLessBookieTest extends BaseTestCase {
    
    protected ServerConfiguration newServerConfiguration() throws Exception {       
        return super
                .newServerConfiguration()
                .setDisableServerSocketBind(true)
                .setEnableLocalTransport(true);
    }
        
    DigestType digestType;
    
    public NetworkLessBookieTest(DigestType digestType) {
        super(4);            
        this.digestType=digestType;
    }

    @Test
    public void testUseLocalBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(20000);

        CountDownLatch l = new CountDownLatch(1);
        zkUtil.sleepServer(5, l);
        l.await();
                
        try (BookKeeper bkc = new BookKeeper(conf);) {
            try (LedgerHandle h = bkc.createLedger(1,1,digestType, "testPasswd".getBytes());) {
                h.addEntry("test".getBytes());
            }
        }

        for (BookieServer bk : bs) {
            for (Channel channel : bk.nettyServer.allChannels) {
                if (!(channel instanceof LocalChannel)) {
                    Assert.fail();
                }
            }
        }
    }
}
