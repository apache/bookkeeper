/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.bookies;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookieInfoReader;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link InfoCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({InfoCommand.class})
public class InfoCommandTest extends BookieCommandTestBase {

    private BookieSocketAddress bookieId;
    private BookieInfoReader.BookieInfo bInfo;
    private BookKeeper bk;
    private Map<BookieSocketAddress, BookieInfoReader.BookieInfo> map = new HashMap<>();

    public InfoCommandTest() {
        super(1, 0);
    }

    @Before
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class)
            .withNoArguments()
            .thenReturn(conf);

        PowerMockito.whenNew(ClientConfiguration.class)
            .withParameterTypes(AbstractConfiguration.class)
            .withArguments(eq(conf))
            .thenReturn(mock(ClientConfiguration.class));

        this.bk = mock(BookKeeper.class);
        PowerMockito.whenNew(BookKeeper.class)
            .withParameterTypes(ClientConfiguration.class)
            .withArguments(any(ClientConfiguration.class))
            .thenReturn(bk);

        this.bookieId = new BookieSocketAddress("localhost", 9999);
        this.bInfo = mock(BookieInfoReader.BookieInfo.class);
        map.put(bookieId, bInfo);
        when(bk.getBookieInfo()).thenReturn(map);
    }

    @Test
    public void testCommand() throws Exception {
        InfoCommand cmd = new InfoCommand();
        cmd.apply(bkFlags, new String[]{""});

        PowerMockito.verifyNew(ClientConfiguration.class, times(1))
            .withArguments(eq(conf));
        PowerMockito.verifyNew(BookKeeper.class, times(1))
            .withArguments(any(ClientConfiguration.class));

        verify(bk, times(1)).getBookieInfo();
        verify(bInfo, times(1 * 3)).getFreeDiskSpace();
        verify(bInfo, times(1 * 3)).getTotalDiskSpace();
    }
}
