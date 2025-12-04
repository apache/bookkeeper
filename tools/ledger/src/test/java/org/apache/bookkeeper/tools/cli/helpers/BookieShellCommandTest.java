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

package org.apache.bookkeeper.tools.cli.helpers;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.Test;

/**
 * Unit test {@link BookieShellCommand}.
 */
public class BookieShellCommandTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testShellCommand() throws Exception {
        BKCommand<CliFlags> command = mock(BKCommand.class);
        String shellCommandName = "test-shell-command";
        CompositeConfiguration conf = new CompositeConfiguration();
        BookieShellCommand<CliFlags> shellCommand = new BookieShellCommand<>(
            shellCommandName,
            command,
            conf);

        // test `description`
        assertEquals(
            shellCommandName + " [options]",
            shellCommand.description());

        // test `printUsage`
        shellCommand.printUsage();
        verify(command, times(1)).usage();

        // test `runCmd`
        String[] args = new String[] { "arg-1", "arg-2" };
        shellCommand.runCmd(args);
        verify(command, times(1))
            .apply(same(shellCommandName), same(conf), same(args));
    }

}
