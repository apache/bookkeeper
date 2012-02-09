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

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.junit.*;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.apache.bookkeeper.test.BaseTestCase;

public class BookieLayoutVersionTest extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(BookieLayoutVersionTest.class);
    
    final int BOOKIE_PORT = 3181;

    public BookieLayoutVersionTest(DigestType digestType) {
        super(0);
    }

    private static void writeTextToVersionFile(File dir, String text) throws IOException {
        File versionFile = new File(dir, Bookie.VERSION_FILENAME);

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(text);
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    private static void writeDirectoryLayoutVersionFile(File dir, int version) throws IOException {
        writeTextToVersionFile(dir, String.valueOf(version));
    }

    private static File newDirectory(int version) throws IOException {
        File d = newDirectoryWithoutVersion();
        writeDirectoryLayoutVersionFile(d, version);
        return d;
    }
    
    private static File newDirectoryWithoutVersion() throws IOException {
        File d = File.createTempFile("bookie", "dir");
        d.delete();
        d.mkdirs();
        return d;
    }

    @Test
    public void testTooNewVersion() throws Exception {
        // test with bad ledger dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectoryWithoutVersion(),
                new File[] { newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION + 1) }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }
        
        // test with bad data dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION + 1),
                new File[] { newDirectoryWithoutVersion() }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }

        // test with both bad        
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION + 1),
                new File[] { newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION + 1) }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }

        // test with neither bad, both with good version
        Bookie b = new Bookie(newServerConfiguration(
            BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION ),
            new File[] { newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION) }));
        b.shutdown();
    }

    @Test
    public void testTooOldVersion() throws Exception {
        // test with bad ledger dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectoryWithoutVersion(),
                new File[] { newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1) }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }
        
        // test with bad data dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1),
                new File[] { newDirectoryWithoutVersion() }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }

        // test with both bad        
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1),
                new File[] { newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1) }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }
    }
    
    @Test
    public void testSomeOldSomeCurrent() throws Exception {
        // test with both bad        
        try {
            Bookie b = new Bookie(newServerConfiguration(
                    BOOKIE_PORT, HOSTPORT, newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION),
                    new File[] { newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1),
                                 newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION),
                                 newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION + 1),
                                 newDirectory(Bookie.CURRENT_DIRECTORY_LAYOUT_VERSION),
                                 newDirectory(Bookie.MIN_COMPAT_DIRECTORY_LAYOUT_VERSION - 1),}));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Directory has an invalid version,"));
        }
    }

    @Test
    public void testInvalidVersionFile() throws Exception {
        // invalid data
        File junkDir = newDirectoryWithoutVersion();
        writeTextToVersionFile(junkDir, "JunkText");
        File junkDir2 = newDirectoryWithoutVersion();
        writeTextToVersionFile(junkDir2, "JunkText2");

        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectoryWithoutVersion(),
                new File[] { junkDir }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Version file has invalid content"));
        }
        
        // test with bad data dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, junkDir,
                new File[] { newDirectoryWithoutVersion() }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Version file has invalid content"));
        }

        // test with both bad        
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, junkDir,
                new File[] { junkDir2 }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Version file has invalid content"));
        }
    }
    
    @Test 
    public void directoryIsReadOnly() throws Exception {
        // invalid data
        File roDir = newDirectoryWithoutVersion();
        roDir.setWritable(false);
        File roDir2 = newDirectoryWithoutVersion();
        roDir2.setWritable(false);

        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, newDirectoryWithoutVersion(),
                new File[] { roDir }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Permission denied"));
        }
        
        // test with bad data dir
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, roDir,
                new File[] { newDirectoryWithoutVersion() }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Permission denied"));
        }

        // test with both bad        
        try {
            Bookie b = new Bookie(newServerConfiguration(
                BOOKIE_PORT, HOSTPORT, roDir,
                new File[] { roDir2 }));
            fail("Shouldn't reach here");
        } catch (IOException ioe) {
            assertTrue("Invalid exception", 
                       ioe.getMessage().contains("Permission denied"));
        }

    } 


}
