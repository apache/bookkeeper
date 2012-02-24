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

import java.io.File;
import java.io.IOException;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.PrintStream;


import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.bookkeeper.conf.ServerConfiguration;

public class UpgradeTest {
    static String newV1JournalDirectory() throws IOException {
        File d = File.createTempFile("bookie", "tmpdir");
        d.delete();
        d.mkdirs();
        new File(d, Long.toHexString(System.currentTimeMillis()) + ".txn").createNewFile();
        return d.getPath();
    }

    static String newV1LedgerDirectory() throws IOException {
        File d = File.createTempFile("bookie", "tmpdir");
        d.delete();
        d.mkdirs();
        new File(d, Long.toHexString(System.currentTimeMillis()) + ".log").createNewFile();
        return d.getPath();
    }

    static void createVersion2File(String dir) throws IOException {
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

    static String newV2JournalDirectory() throws IOException {
        String d = newV1JournalDirectory();
        createVersion2File(d);
        return d;
    }

    static String newV2LedgerDirectory() throws IOException {
        String d = newV1LedgerDirectory();
        createVersion2File(d);
        return d;
    }

    @Test
    public void testCommandLine() throws Exception {
        PrintStream origerr = System.err;
        PrintStream origout = System.out;

        File output = File.createTempFile("bookie", "tmpout");
        System.setOut(new PrintStream(output));
        System.setErr(new PrintStream(output));
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