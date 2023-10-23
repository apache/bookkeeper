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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.CookieCommandTestBase;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test {@link UpdateCookieCommand}.
 */
public class UpdateCookieCommandTest extends CookieCommandTestBase {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final PrintStream console = new PrintStream(output);

    private boolean runCommand(String[] args) {
        UpdateCookieCommand updateCmd = new UpdateCookieCommand(console);
        BKFlags bkFlags = new BKFlags();
        bkFlags.serviceUri = "zk://127.0.0.1";
        return updateCmd.apply(bkFlags, args);
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

    private void assertPrintUsage(String consoleOutput) {
        assertPrintUsage(consoleOutput, "cookie_update [options]");
    }

    /**
     * Run a command without cookie file.
     */
    @Test
    public void testMissingCookieFileOption() {
        assertFalse(runCommand(new String[] { BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertOptionMissing(consoleOutput, "[-cf | --cookie-file]");
        assertPrintUsage(consoleOutput);
    }

    /**
     * Run a command with invalid bookie id.
     */
    @Test
    public void testInvalidBookieId() {
        assertFalse(runCommand(new String[] { "-cf", "test-cookie-file", INVALID_BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertInvalidBookieId(consoleOutput, INVALID_BOOKIE_ID);
    }

    /**
     * Run a command with a non-existent cookie file.
     */
    @Test
    public void testUpdateCookieFromNonExistentCookieFile() {
        String file = "/path/to/non-existent-cookie-file";
        assertFalse(runCommand(new String[] { "-cf", file, BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertCookieFileNotExists(consoleOutput, file);
    }

    /**
     * A successful run.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateCookieFromExistentCookieFile() throws Exception {
        File file = testFolder.newFile("test-cookie-file");
        byte[] content = "test-update-cookie".getBytes(UTF_8);
        Files.write(Paths.get(file.toURI()), content);
        String fileName = file.getPath();
        assertTrue(runCommand(new String[] { "-cf", fileName, BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertTrue(consoleOutput, consoleOutput.isEmpty());
        verify(rm, times(1)).writeCookie(eq(BookieId.parse(BOOKIE_ID)), any(Versioned.class));
    }

    /**
     * Run a command to update cookie on an non-existent cookie.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateNonExistedCookie() throws Exception {
        doThrow(new CookieNotFoundException())
            .when(rm).writeCookie(eq(BookieId.parse(BOOKIE_ID)), any(Versioned.class));

        File file = testFolder.newFile("test-cookie-file");
        byte[] content = "test-update-cookie".getBytes(UTF_8);
        Files.write(Paths.get(file.toURI()), content);
        String fileName = file.getPath();
        assertFalse(runCommand(new String[] { "-cf", fileName, BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Cookie not found for bookie '" + BOOKIE_ID + "'"));
        verify(rm, times(1)).writeCookie(eq(BookieId.parse(BOOKIE_ID)), any(Versioned.class));
    }

    /**
     * Run a command to update cookie when exception is thrown.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateCookieException() throws Exception {
        doThrow(new OperationRejectedException())
            .when(rm).writeCookie(eq(BookieId.parse(BOOKIE_ID)), any(Versioned.class));

        File file = testFolder.newFile("test-cookie-file");
        byte[] content = "test-update-cookie".getBytes(UTF_8);
        Files.write(Paths.get(file.toURI()), content);
        String fileName = file.getPath();
        assertFalse(runCommand(new String[] { "-cf", fileName, BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Exception on updating cookie for bookie '" + BOOKIE_ID + "'"));
        assertTrue(
            consoleOutput,
            consoleOutput.contains(OperationRejectedException.class.getName()));
        verify(rm, times(1)).writeCookie(eq(BookieId.parse(BOOKIE_ID)), any(Versioned.class));
    }

}
