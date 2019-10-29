/*
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
 */

package org.apache.bookkeeper.tools.cli.commands.cookie;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tools.cli.helpers.CookieCommandTestBase;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test {@link GetCookieCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ MetadataDrivers.class })
public class GenerateCookieCommandTest extends CookieCommandTestBase {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final PrintStream console = new PrintStream(output);

    private boolean runCommand(String[] args) {
        GenerateCookieCommand getCmd = new GenerateCookieCommand(console);
        BKFlags bkFlags = new BKFlags();
        bkFlags.serviceUri = "zk://127.0.0.1";
        return getCmd.apply(bkFlags, args);
    }

    private String getConsoleOutput() {
        return new String(output.toByteArray(), UTF_8);
    }

    /**
     * Run a command without providing bookie id.
     */
    @Test
    public void testMissingBookieId() {
        assertFalse(runCommand(new String[] {}));
        String consoleOutput = getConsoleOutput();
        assertBookieIdMissing(consoleOutput);
    }

    /**
     * Run a command with invalid bookie id.
     */
    @Test
    public void testInvalidBookieId() {
        assertFalse(runCommand(new String[] {
            "-j", "/path/to/journal",
            "-l", "/path/to/ledgers",
            "-o", "/path/to/cookie-file",
            INVALID_BOOKIE_ID
        }));
        String consoleOutput = getConsoleOutput();
        assertInvalidBookieId(consoleOutput, INVALID_BOOKIE_ID);
    }

    /**
     * Run a command without journal dirs.
     */
    @Test
    public void testMissingJournalDir() {
        assertFalse(runCommand(new String[] { "-l", "/path/to/ledgers", "-o", "/path/to/cookie-file", BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertOptionMissing(consoleOutput, "-j, --journal-dirs");
    }

    /**
     * Run a command without ledger dirs.
     */
    @Test
    public void testMissingLedgerDirs() {
        assertFalse(runCommand(new String[] { "-j", "/path/to/journal", "-o", "/path/to/cookie-file", BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertOptionMissing(consoleOutput, "-l, --ledger-dirs");
    }

    /**
     * Run a command without output file.
     */
    @Test
    public void testMissingOutputFile() {
        assertFalse(runCommand(new String[] { "-j", "/path/to/journal", "-l", "/path/to/ledgers", BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertOptionMissing(consoleOutput, "-o, --output-file");
    }

    /**
     * A successful run without instance id.
     */
    @Test
    public void testGenerateCookieWithoutInstanceId() throws Exception {
        File cookieFile = testFolder.newFile("cookie-without-instance-id");
        String journalDir = "/path/to/journal";
        String ledgersDir = "/path/to/ledgers";
        String instanceId = "test-instance-id";

        Cookie cookie = Cookie.newBuilder()
            .setBookieHost(BOOKIE_ID)
            .setInstanceId(instanceId)
            .setJournalDirs(journalDir)
            .setLedgerDirs(Cookie.encodeDirPaths(ledgersDir.split(",")))
            .build();

        when(rm.getClusterInstanceId()).thenReturn(instanceId);
        assertTrue(
            getConsoleOutput(),
            runCommand(new String[] {
                "-l", ledgersDir,
                "-j", journalDir,
                "-o", cookieFile.getPath(),
                BOOKIE_ID
            }));
        String consoleOutput = getConsoleOutput();
        assertTrue(consoleOutput, consoleOutput.contains(
            "Successfully saved the generated cookie to " + cookieFile.getPath()
        ));
        verify(rm, times(1)).getClusterInstanceId();

        byte[] data = Files.readAllBytes(Paths.get(cookieFile.getPath()));
        assertArrayEquals(cookie.toString().getBytes(UTF_8), data);
    }

    /**
     * A successful run with instance id.
     */
    @Test
    public void testGenerateCookieWithInstanceId() throws Exception {
        File cookieFile = testFolder.newFile("cookie-with-instance-id");
        String journalDir = "/path/to/journal";
        String ledgersDir = "/path/to/ledgers";
        String instanceId = "test-instance-id";

        Cookie cookie = Cookie.newBuilder()
            .setBookieHost(BOOKIE_ID)
            .setInstanceId(instanceId)
            .setJournalDirs(journalDir)
            .setLedgerDirs(Cookie.encodeDirPaths(ledgersDir.split(",")))
            .build();

        when(rm.getClusterInstanceId()).thenReturn(instanceId);
        assertTrue(
            getConsoleOutput(),
            runCommand(new String[] {
                "-l", ledgersDir,
                "-j", journalDir,
                "-o", cookieFile.getPath(),
                "-i", instanceId,
                BOOKIE_ID
            }));
        String consoleOutput = getConsoleOutput();
        assertTrue(consoleOutput, consoleOutput.contains(
            "Successfully saved the generated cookie to " + cookieFile.getPath()
        ));
        verify(rm, times(0)).getClusterInstanceId();

        byte[] data = Files.readAllBytes(Paths.get(cookieFile.getPath()));
        assertArrayEquals(cookie.toString().getBytes(UTF_8), data);
    }

    /**
     * A successful run with encoding multiple ledgers while generating cookie.
     */
    @Test
    public void testGenerateCookieWithMultipleLedgerDirs() throws Exception {
        File cookieFile = testFolder.newFile("cookie-with-instance-id");
        String journalDir = "/path/to/journal";
        String ledgersDir = "/path/to/ledgers,/path/to/more/ledgers";
        String instanceId = "test-instance-id";

        when(rm.getClusterInstanceId()).thenReturn(instanceId);
        assertTrue(
                getConsoleOutput(),
                runCommand(new String[] {
                        "-l", ledgersDir,
                        "-j", journalDir,
                        "-o", cookieFile.getPath(),
                        "-i", instanceId,
                        BOOKIE_ID
                }));
        String consoleOutput = getConsoleOutput();
        assertTrue(consoleOutput, consoleOutput.contains(
                "Successfully saved the generated cookie to " + cookieFile.getPath()
        ));
        verify(rm, times(0)).getClusterInstanceId();

        List<String> cookieFields = Files.readAllLines(Paths.get(cookieFile.getPath()));
        for (String cookieField : cookieFields) {
            String[] fields = cookieField.split(" ");
            if (fields[0].equals("ledgerDirs:")) {
                assertEquals(fields[1].charAt(1), '2');
            }
        }
    }

}
