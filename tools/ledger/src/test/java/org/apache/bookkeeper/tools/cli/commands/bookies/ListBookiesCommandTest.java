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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.value;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.discover.BookieServiceInfoUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.tools.cli.helpers.DiscoveryCommandTestBase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link ListBookiesCommand}.
 */
public class ListBookiesCommandTest extends DiscoveryCommandTestBase {

    private static class BookieAddressComparator implements Comparator<BookieId> {

        @Override
        public int compare(BookieId o1, BookieId o2) {
            return o1.toString().compareToIgnoreCase(o2.toString());
        }
    }

    private Set<BookieId> writableBookies;
    private Set<BookieId> readonlyBookies;
    private Set<BookieId> allBookies;

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
        when(regClient.getBookieServiceInfo(any(BookieId.class)))
            .thenReturn(value(new Versioned<>(
                    BookieServiceInfoUtils.buildLegacyBookieServiceInfo("localhost:1234"), new LongVersion(0))));
        when(regClient.getReadOnlyBookies())
            .thenReturn(value(new Versioned<>(readonlyBookies, new LongVersion(0L))));
        when(regClient.getAllBookies())
            .thenReturn(value(new Versioned<>(allBookies, new LongVersion(0L))));

        mockStatic(CommandHelpers.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    }

    private static Set<BookieId> createBookies(int startPort, int numBookies) {
        Set<BookieId> bookies = new TreeSet<>(new BookieAddressComparator());
        int i = 0;
        for (; i < numBookies - 1; i++) {
            bookies.add(BookieId.parse("127.0.0.1" + (startPort + i)));
        }
        // mix an unknown hostname bookieId
        bookies.add(BookieId.parse("unknown" + (startPort + i)));
        return bookies;
    }

    private void verifyPrintBookies(int startPort, int numBookies, int numCalls) {
        for (int i = 0; i < numBookies; i++) {
            if (i == numBookies - 1){
                final BookieId expectedBookieId = BookieId.parse("unknown" + (startPort + i));
                getMockedStatic(CommandHelpers.class)
                    .verify(() -> CommandHelpers.getBookieSocketAddrStringRepresentation(
                                        eq(expectedBookieId),
                                        any(BookieAddressResolver.class)),
                                times(numCalls));
            } else {
                final BookieId expectedBookieId = BookieId.parse("127.0.0.1" + (startPort + i));
                getMockedStatic(CommandHelpers.class)
                        .verify(() -> CommandHelpers.getBookieSocketAddrStringRepresentation(
                                        eq(expectedBookieId),
                                        any(BookieAddressResolver.class)),
                                times(numCalls));
            }
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
            e.printStackTrace();
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

        getMockedStatic(CommandHelpers.class)
                .verify(() -> CommandHelpers.getBookieSocketAddrStringRepresentation(any(), any()),
                        times(0));

        assertTrue(cmd.apply(bkFlags, new String[]{"-ro"}));

        getMockedStatic(CommandHelpers.class)
                .verify(() -> CommandHelpers.getBookieSocketAddrStringRepresentation(any(), any()),
                times(0));
    }

}
