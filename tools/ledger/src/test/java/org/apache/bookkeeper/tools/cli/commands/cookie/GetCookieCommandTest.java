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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.CookieCommandTestBase;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
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
public class GetCookieCommandTest extends CookieCommandTestBase {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final PrintStream console = new PrintStream(output);

    private boolean runCommand(String[] args) {
        GetCookieCommand getCmd = new GetCookieCommand(console);
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
        assertFalse(runCommand(new String[] { INVALID_BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertInvalidBookieId(consoleOutput, INVALID_BOOKIE_ID);
    }

    /**
     * A successful run.
     */
    @Test
    public void testGetCookieFromExistentCookieFile() throws Exception {
        Cookie cookie = Cookie.newBuilder()
            .setBookieHost(BOOKIE_ID)
            .setInstanceId("test-instance-id")
            .setJournalDirs("/path/to/journal/dir")
            .setLedgerDirs("/path/to/ledger/dirs")
            .build();
        when(rm.readCookie(eq(BookieId.parse(BOOKIE_ID))))
            .thenReturn(new Versioned<>(cookie.toString().getBytes(UTF_8), new LongVersion(-1L)));
        assertTrue(runCommand(new String[] { BOOKIE_ID.toString() }));
        String consoleOutput = getConsoleOutput();
        assertTrue(consoleOutput, consoleOutput.contains(cookie.toString()));
        verify(rm, times(1)).readCookie(eq(BookieId.parse(BOOKIE_ID)));
    }

    /**
     * Run a command to get cookie on an non-existent cookie.
     */
    @Test
    public void testGetNonExistedCookie() throws Exception {
        doThrow(new CookieNotFoundException())
            .when(rm).readCookie(eq(BookieId.parse(BOOKIE_ID)));

        assertFalse(runCommand(new String[] { BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Cookie not found for bookie '" + BOOKIE_ID + "'"));
        verify(rm, times(1)).readCookie(eq(BookieId.parse(BOOKIE_ID)));
    }

    /**
     * Run a command to get cookie when exception is thrown.
     */
    @Test
    public void testGetCookieException() throws Exception {
        doThrow(new OperationRejectedException())
            .when(rm).readCookie(eq(BookieId.parse(BOOKIE_ID)));

        assertFalse(runCommand(new String[] { BOOKIE_ID }));
        String consoleOutput = getConsoleOutput();
        assertTrue(
            consoleOutput,
            consoleOutput.contains("Exception on getting cookie for bookie '" + BOOKIE_ID + "'"));
        assertTrue(
            consoleOutput,
            consoleOutput.contains(OperationRejectedException.class.getName()));
        verify(rm, times(1)).readCookie(eq(BookieId.parse(BOOKIE_ID)));
    }

}
