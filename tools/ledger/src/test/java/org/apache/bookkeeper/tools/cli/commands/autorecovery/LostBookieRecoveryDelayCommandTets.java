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
package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link LostBookieRecoveryDelayCommand}.
 */
public class LostBookieRecoveryDelayCommandTets extends BookieCommandTestBase {

    public LostBookieRecoveryDelayCommandTets() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockClientConfigurationConstruction();
        mockBookKeeperAdminConstruction(
                new Consumer<BookKeeperAdmin>() {
                    @Override
                    @SneakyThrows
                    public void accept(BookKeeperAdmin bookKeeperAdmin) {
                        when(bookKeeperAdmin.getLostBookieRecoveryDelay()).thenReturn(1);
                        doNothing().when(bookKeeperAdmin).setLostBookieRecoveryDelay(anyInt());
                    }
                }
        );

    }

    @Test
    public void testWithoutArgs() {
        LostBookieRecoveryDelayCommand cmd = new LostBookieRecoveryDelayCommand();
        Assert.assertFalse(cmd.apply(bkFlags, new String[]{""}));
    }

    @Test
    public void testWithSet() throws Exception {
        testCommand("-s", "1");
        verify(getMockedConstruction(BookKeeperAdmin.class).constructed().get(0),
                times(1)).setLostBookieRecoveryDelay(1);
    }

    @Test
    public void testWithGet() throws Exception {
        testCommand("-g");
        verify(getMockedConstruction(BookKeeperAdmin.class).constructed().get(0),
                times(1)).getLostBookieRecoveryDelay();
    }

    private void testCommand(String... args) {
        LostBookieRecoveryDelayCommand cmd = new LostBookieRecoveryDelayCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
