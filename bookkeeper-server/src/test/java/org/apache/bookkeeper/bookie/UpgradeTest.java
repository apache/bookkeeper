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

package org.apache.bookkeeper.bookie;

import java.util.Arrays;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.io.File;
import java.io.IOException;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;

import org.apache.zookeeper.ZooKeeper;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.test.PortManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeTest {
    static Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    ZooKeeperUtil zkutil;
    ZooKeeper zkc = null;
    final static int bookiePort = PortManager.nextFreePort();

    @Before
    public void setupZooKeeper() throws Exception {
        zkutil = new ZooKeeperUtil();
        zkutil.startServer();
        zkc = zkutil.getZooKeeperClient();
    }

    @After
    public void tearDownZooKeeper() throws Exception {
        zkutil.killServer();
    }

    static void writeLedgerDir(File dir,
                               byte[] masterKey)
            throws Exception {
        long ledgerId = 1;

        File fn = new File(dir, LedgerCacheImpl.getLedgerName(ledgerId));
        fn.getParentFile().mkdirs();
        FileInfo fi = new FileInfo(fn, masterKey);
        // force creation of index file
        fi.write(new ByteBuffer[]{ ByteBuffer.allocate(0) }, 0);
        fi.close(true);

        long logId = 0;
        ByteBuffer LOGFILE_HEADER = ByteBuffer.allocate(1024);
        LOGFILE_HEADER.put("BKLO".getBytes());
        FileChannel logfile = new RandomAccessFile(
                new File(dir, Long.toHexString(logId)+".log"), "rw").getChannel();
        logfile.write((ByteBuffer) LOGFILE_HEADER.clear());
        logfile.close();
    }

    static JournalChannel writeJournal(File journalDir, int numEntries, byte[] masterKey)
            throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        BufferedChannel bc = jc.getBufferedChannel();

        long ledgerId = 1;
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;

        for (int i = 1; i <= numEntries; i++) {
            ByteBuffer packet = ClientUtil.generatePacket(ledgerId, i, lastConfirmed,
                                                          i*data.length, data).toByteBuffer();
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.remaining());
            lenBuff.flip();

            bc.write(lenBuff);
            bc.write(packet);
        }
        bc.flush(true);

        return jc;
    }

    static String newV1JournalDirectory() throws Exception {
        File d = File.createTempFile("bookie", "tmpdir");
        d.delete();
        d.mkdirs();
        writeJournal(d, 100, "foobar".getBytes()).close();
        return d.getPath();
    }

    static String newV1LedgerDirectory() throws Exception {
        File d = File.createTempFile("bookie", "tmpdir");
        d.delete();
        d.mkdirs();
        writeLedgerDir(d, "foobar".getBytes());
        return d.getPath();
    }

    static void createVersion2File(String dir) throws Exception {
        File versionFile = new File(dir, "VERSION");

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(String.valueOf(2));
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    static String newV2JournalDirectory() throws Exception {
        String d = newV1JournalDirectory();
        createVersion2File(d);
        return d;
    }

    static String newV2LedgerDirectory() throws Exception {
        String d = newV1LedgerDirectory();
        createVersion2File(d);
        return d;
    }

    private static void testUpgradeProceedure(String zkServers, String journalDir, String ledgerDir) throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkServers)
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(bookiePort);
        Bookie b = null;
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException e) {
            // correct behaviour
            assertTrue("wrong exception", e.getMessage().contains("upgrade needed"));
        }

        FileSystemUpgrade.upgrade(conf); // should work fine
        b = new Bookie(conf);
        b.start();
        b.shutdown();
        b = null;

        FileSystemUpgrade.rollback(conf);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException e) {
            // correct behaviour
            assertTrue("wrong exception", e.getMessage().contains("upgrade needed"));
        }

        FileSystemUpgrade.upgrade(conf);
        FileSystemUpgrade.finalizeUpgrade(conf);
        b = new Bookie(conf);
        b.start();
        b.shutdown();
        b = null;
    }

    @Test(timeout=60000)
    public void testUpgradeV1toCurrent() throws Exception {
        String journalDir = newV1JournalDirectory();
        String ledgerDir = newV1LedgerDirectory();
        testUpgradeProceedure(zkutil.getZooKeeperConnectString(), journalDir, ledgerDir);
    }

    @Test(timeout=60000)
    public void testUpgradeV2toCurrent() throws Exception {
        String journalDir = newV2JournalDirectory();
        String ledgerDir = newV2LedgerDirectory();
        testUpgradeProceedure(zkutil.getZooKeeperConnectString(), journalDir, ledgerDir);
    }

    @Test(timeout=60000)
    public void testUpgradeCurrent() throws Exception {
        String journalDir = newV2JournalDirectory();
        String ledgerDir = newV2LedgerDirectory();
        testUpgradeProceedure(zkutil.getZooKeeperConnectString(), journalDir, ledgerDir);
        // Upgrade again
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(bookiePort);
        FileSystemUpgrade.upgrade(conf); // should work fine with current directory
        Bookie b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    @Test(timeout=60000)
    public void testCommandLine() throws Exception {
        PrintStream origerr = System.err;
        PrintStream origout = System.out;

        File output = File.createTempFile("bookie", "stdout");
        File erroutput = File.createTempFile("bookie", "stderr");
        System.setOut(new PrintStream(output));
        System.setErr(new PrintStream(erroutput));
        try {
            FileSystemUpgrade.main(new String[] { "-h" });
            try {
                // test without conf
                FileSystemUpgrade.main(new String[] { "-u" });
                fail("Should have failed");
            } catch (IllegalArgumentException iae) {
                assertTrue("Wrong exception " + iae.getMessage(),
                           iae.getMessage().contains("without configuration"));
            }
            File f = File.createTempFile("bookie", "tmpconf");
            try {
                // test without upgrade op
                FileSystemUpgrade.main(new String[] { "--conf", f.getPath() });
                fail("Should have failed");
            } catch (IllegalArgumentException iae) {
                assertTrue("Wrong exception " + iae.getMessage(),
                           iae.getMessage().contains("Must specify -upgrade"));
            }
        } finally {
            System.setOut(origout);
            System.setErr(origerr);
        }
    }
}
