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
package org.apache.bookkeeper.tools.cli.commands.bookies;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.value;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.google.common.primitives.UnsignedBytes;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.tools.cli.helpers.DiscoveryCommandTestBase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link ListBookiesCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ListBookiesCommand.class, CommandHelpers.class })
public class ListBookiesCommandTest extends DiscoveryCommandTestBase {

    private static class BookieAddressComparator implements Comparator<BookieSocketAddress> {

        @Override
        public int compare(BookieSocketAddress o1, BookieSocketAddress o2) {
            int ret = UnsignedBytes.lexicographicalComparator()
                .compare(o1.getHostName().getBytes(UTF_8), o2.getHostName().getBytes(UTF_8));
            if (ret == 0) {
                return Integer.compare(o1.getPort(), o2.getPort());
            } else {
                return ret;
            }
        }
    }

    private Set<BookieSocketAddress> writableBookies;
    private Set<BookieSocketAddress> readonlyBookies;
    private Set<BookieSocketAddress> allBookies;

    @Before
    public void setup() throws Exception {
        super.setup();

        writableBookies = createBookies(3181, 10);
        readonlyBookies = createBookies(4181, 10);

        allBookies = new HashSet<>();
        allBookies.addAll(writableBookies);
        allBookies.addAll(readonlyBookies);

        when(regClient.getWritableBookies())
            .thenReturn(value(new Versioned<>(writableBookies, new LongVersion(0L))));
        when(regClient.getReadOnlyBookies())
            .thenReturn(value(new Versioned<>(readonlyBookies, new LongVersion(0L))));
        when(regClient.getAllBookies())
            .thenReturn(value(new Versioned<>(allBookies, new LongVersion(0L))));

        PowerMockito.mockStatic(CommandHelpers.class, CALLS_REAL_METHODS);
    }

    private static Set<BookieSocketAddress> createBookies(int startPort, int numBookies) {
        Set<BookieSocketAddress> bookies = new TreeSet<>(new BookieAddressComparator());
        for (int i = 0; i < numBookies; i++) {
            bookies.add(new BookieSocketAddress("127.0.0.1", startPort + i));
        }
        return bookies;
    }

    private static void verifyPrintBookies(int startPort, int numBookies, int numCalls) {
        for (int i = 0; i < numBookies; i++) {
            PowerMockito.verifyStatic(
                CommandHelpers.class,
                times(numCalls));
            CommandHelpers.getBookieSocketAddrStringRepresentation(
                eq(new BookieSocketAddress("127.0.0.1", startPort + 1)));
        }
    }

    @Test
    public void testListReadWriteShortArgs() {
        testCommand(false, true, false,
            "list",
            "-rw");
    }

    @Test
    public void testListReadWriteLongArgs() {
        testCommand(false, true, false,
            "list",
            "--readwrite");
    }

    @Test
    public void testListReadOnlyShortArgs() {
        testCommand(false, false, true,
            "list",
            "-ro");
    }

    @Test
    public void testListAllLongArgs() {
        testCommand(true, false, false,
            "list",
            "--all");
    }

    @Test
    public void testListAllShortArgs() {
        testCommand(true, false, false,
            "list",
            "-a");
    }

    @Test
    public void testListReadOnlyLongArgs() {
        testCommand(false, false, true,
            "list",
            "--readonly");
    }

    @Test
    public void testListNoArgs() {
        testCommand(true, true, true,
            "list");
    }

    @Test
    public void testListTwoFlagsCoexistsShortArgs() {
        testCommand(false, true, true,
            "list", "-rw", "-ro");
    }

    @Test
    public void testListTwoFlagsCoexistsLongArgs() {
        testCommand(false, true, true,
            "list", "--readwrite", "--readonly");
    }

    private void testCommand(boolean all,
                             boolean readwrite,
                             boolean readonly,
                             String... args) {

        ListBookiesCommand cmd = new ListBookiesCommand();
        try {
            assertTrue(cmd.apply(bkFlags, args));
        } catch (Exception e) {
            fail("Should not throw any exception here");
        }

        if (all) {
            if (readwrite && readonly) {
                verifyPrintBookies(3181, 10, 2);
                verifyPrintBookies(4181, 10, 2);
            } else if (readwrite && !readonly) {
                verifyPrintBookies(3181, 10, 2);
                verifyPrintBookies(4181, 10, 1);
            } else if (readonly && !readwrite) {
                verifyPrintBookies(3181, 10, 1);
                verifyPrintBookies(4181, 10, 2);
            } else {
                verifyPrintBookies(3181, 10, 1);
                verifyPrintBookies(4181, 10, 1);
            }
        } else if (readwrite && !readonly) {
            verifyPrintBookies(3181, 10, 1);
            verifyPrintBookies(4181, 10, 0);
        } else if (readonly && !readwrite) {
            verifyPrintBookies(3181, 10, 0);
            verifyPrintBookies(4181, 10, 1);
        } else {
            verifyPrintBookies(3181, 10, 1);
            verifyPrintBookies(4181, 10, 1);
        }
    }

    @Test
    public void testListEmptyBookies() throws Exception {
        // overwrite regClient to return empty bookies
        when(regClient.getWritableBookies())
            .thenReturn(value(new Versioned<>(Collections.emptySet(), new LongVersion(0L))));
        when(regClient.getReadOnlyBookies())
            .thenReturn(value(new Versioned<>(Collections.emptySet(), new LongVersion(0L))));

        ListBookiesCommand cmd = new ListBookiesCommand();
        assertTrue(cmd.apply(bkFlags, new String[] { "-rw"}));

        PowerMockito.verifyStatic(
                CommandHelpers.class,
                times(0));
        CommandHelpers.getBookieSocketAddrStringRepresentation(any());

        assertTrue(cmd.apply(bkFlags, new String[]{"-ro"}));

        PowerMockito.verifyStatic(
                CommandHelpers.class,
                times(0));
        CommandHelpers.getBookieSocketAddrStringRepresentation(any());
    }

}
