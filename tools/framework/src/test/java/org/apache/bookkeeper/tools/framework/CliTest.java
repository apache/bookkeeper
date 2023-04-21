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

package org.apache.bookkeeper.tools.framework;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.tools.framework.TestCli.TestNestedFlags;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing the Cli framework using {@link TestCli}.
 */
public class CliTest {

    private ByteArrayOutputStream consoleBuffer;
    private PrintStream console;

    @Before
    public void setup() throws Exception {
        this.consoleBuffer = new ByteArrayOutputStream();
        this.console = new PrintStream(consoleBuffer, true, UTF_8.name());
    }

    @Test
    public void testEmptyArgs() {
        assertEquals(-1, TestCli.doMain(console, new String[] {}));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains("Usage:"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("Flags:"));
    }

    @Test
    public void testHelpCommand() {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "help"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains("Usage:"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("Flags:"));
    }

    @Test
    public void testHelpFlag() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "help"
        }));
        String buffer1 = new String(consoleBuffer.toByteArray(), UTF_8);

        ByteArrayOutputStream anotherBuffer = new ByteArrayOutputStream();
        PrintStream anotherConsole = new PrintStream(anotherBuffer, true, UTF_8.name());

        assertEquals(-1, TestCli.doMain(anotherConsole, new String[] {
            "-h"
        }));
        String buffer2 = new String(anotherBuffer.toByteArray(), UTF_8);

        assertEquals(buffer1, buffer2);
    }

    @Test
    public void testPrintCommandUsage() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "help", "dog"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        ByteArrayOutputStream expectedBufferStream = new ByteArrayOutputStream();
        PrintStream expectedConsole = new PrintStream(expectedBufferStream, true, UTF_8.name());
        expectedConsole.println("command dog");
        String expectedBuffer = new String(expectedBufferStream.toByteArray(), UTF_8);

        assertEquals(expectedBuffer, consoleBufferStr);
    }

    @Test
    public void testPrintHelpCommandUsage() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "help", "help"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains(
            "Usage:  bktest help [command] [options]"
        ));
    }

    @Test
    public void testInvalidFlags() throws Exception {
        assertEquals(-1, TestCli.doMain(console, new String[] {
            "-s", "string",
            "-i", "1234",
            "-l"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains("Error : Expected a value after parameter -l"));
        // help message should be printed
        assertTrue(consoleBufferStr.contains("Usage:"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("Flags:"));
    }

    @Test
    public void testNestedCommandEmptySubCommand() {
        assertEquals(-1, TestCli.doMain(console, new String[] {
            "nested"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        // should print usage of 'nested' command
        assertTrue(consoleBufferStr.contains("Usage:  bktest nested"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("cat"));
        assertTrue(consoleBufferStr.contains("fish"));
        assertTrue(consoleBufferStr.contains("Flags:"));
        assertTrue(consoleBufferStr.contains("--nested-int-flag"));
    }

    @Test
    public void testNestedCommandHelpCommand() {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "nested", "help"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        // should print usage of 'nested' command
        assertTrue(consoleBufferStr.contains("Usage:  bktest nested"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("cat"));
        assertTrue(consoleBufferStr.contains("fish"));
        assertTrue(consoleBufferStr.contains("Flags:"));
        assertTrue(consoleBufferStr.contains("--nested-int-flag"));
    }

    @Test
    public void testNestedCommandHelpFlag() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "nested", "help"
        }));
        String buffer1 = new String(consoleBuffer.toByteArray(), UTF_8);

        ByteArrayOutputStream anotherBuffer = new ByteArrayOutputStream();
        PrintStream anotherConsole = new PrintStream(anotherBuffer, true, UTF_8.name());

        assertEquals(-1, TestCli.doMain(anotherConsole, new String[] {
            "nested", "-h"
        }));
        String buffer2 = new String(anotherBuffer.toByteArray(), UTF_8);

        assertEquals(buffer1, buffer2);
    }

    @Test
    public void testPrintSubCommandUsage() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "nested", "help", "cat"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        ByteArrayOutputStream expectedBufferStream = new ByteArrayOutputStream();
        PrintStream expectedConsole = new PrintStream(expectedBufferStream, true, UTF_8.name());
        expectedConsole.println("command cat");
        String expectedBuffer = new String(expectedBufferStream.toByteArray(), UTF_8);

        assertEquals(expectedBuffer, consoleBufferStr);
    }

    @Test
    public void testPrintHelpSubCommandUsage() throws Exception {
        assertEquals(0, TestCli.doMain(console, new String[] {
            "nested", "help", "help"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains(
            "Usage:  bktest nested help [command] [options]"
        ));
    }

    @Test
    public void testSubCommandInvalidFlags() throws Exception {
        assertEquals(-1, TestCli.doMain(console, new String[] {
            "nested",
            "-s", "string",
            "-i", "1234",
            "-l"
        }));

        String consoleBufferStr = new String(consoleBuffer.toByteArray(), UTF_8);

        assertTrue(consoleBufferStr.contains("Error : Expected a value after parameter -l"));
        // help message should be printed
        assertTrue(consoleBufferStr.contains("Usage:  bktest nested"));
        assertTrue(consoleBufferStr.contains("Commands:"));
        assertTrue(consoleBufferStr.contains("cat"));
        assertTrue(consoleBufferStr.contains("fish"));
        assertTrue(consoleBufferStr.contains("Flags:"));
        assertTrue(consoleBufferStr.contains("--nested-int-flag"));
    }

    @Test
    public void testSubCommandFlags() throws Exception {
        CompletableFuture<TestNestedFlags> flagsFuture = FutureUtils.createFuture();
        assertEquals(0, TestCli.doMain(console, new String[] {
            "nested",
            "-s", "string",
            "-i", "1234",
            "-l", "str1,str2,str3",
            "additional-args"
        }, (flags) -> flagsFuture.complete(flags)));

        dumpConsole();

        TestNestedFlags flags = FutureUtils.result(flagsFuture);
        assertEquals("string", flags.stringFlag);
        assertEquals(1234, flags.intFlag);
        Assert.assertEquals(
            Lists.newArrayList("str1", "str2", "str3"),
            flags.listFlag
        );
        assertEquals(1, flags.arguments.size());
        Assert.assertEquals(Lists.newArrayList("additional-args"), flags.arguments);
    }

    //
    // Util functions
    //

    private void dumpConsole() {
        String buffer = new String(
            consoleBuffer.toByteArray(), UTF_8);

        System.out.println(buffer);
    }

}
