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

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Format the bookkeeper metadata present in zookeeper.
 */
public class MetaFormatCommand extends BookieCommand<MetaFormatCommand.MetaFormatFlags> {

    private static final String NAME = "metaformat";
    private static final String DESC = "Format bookkeeper metadata in zookeeper.";

    public MetaFormatCommand() {
        this(new MetaFormatFlags());
    }

    private MetaFormatCommand(MetaFormatFlags flags) {
        super(CliSpec.<MetaFormatCommand.MetaFormatFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for command meta format.
     */
    @Accessors(fluent = true)
    @Setter
    public static class MetaFormatFlags extends CliFlags {

        @Parameter(names = { "-n", "nonInteractive" }, description = "Whether to confirm old data exists..?")
        private boolean interactive;

        @Parameter(names = {"-f", "--force"},
            description = "If [nonInteractive] is specified, then whether to force delete the old data without prompt.")
        private boolean force;
    }

    @Override
    public boolean apply(ServerConfiguration conf, MetaFormatFlags flags) {
        try {
            return BookKeeperAdmin.format(conf, flags.interactive, flags.force);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
