/*
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
package org.apache.bookkeeper.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.util.Enumeration;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.tests.BookKeeperClusterUtils;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Arquillian.class)
public class TestSmoke {
    private static final Logger LOG = LoggerFactory.getLogger(TestSmoke.class);
    private static byte[] PASSWD = "foobar".getBytes();

    @ArquillianResource
    DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Before
    public void before() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
    }

    @Test
    public void testBootWriteReadShutdown() throws Exception {
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));

        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        BookKeeper bk = new BookKeeper(zookeeper);
        long ledgerId;
        try (LedgerHandle writelh = bk.createLedger(BookKeeper.DigestType.CRC32, PASSWD)) {
            ledgerId = writelh.getId();
            for (int i = 0; i < 100; i++) {
                writelh.addEntry(("entry-" + i).getBytes());
            }
        }

        try (LedgerHandle readlh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWD)) {
            long lac = readlh.getLastAddConfirmed();
            int i = 0;
            Enumeration<LedgerEntry> entries = readlh.readEntries(0, lac);
            while (entries.hasMoreElements()) {
                LedgerEntry e = entries.nextElement();
                String readBack = new String(e.getEntry());
                Assert.assertEquals(readBack, "entry-" + i++);
            }
            Assert.assertEquals(i, 100);
        }

        bk.close();

        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

}
