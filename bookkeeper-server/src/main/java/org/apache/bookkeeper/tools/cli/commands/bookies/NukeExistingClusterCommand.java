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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nuke bookkeeper metadata of existing cluster in zookeeper.
 */
public class NukeExistingClusterCommand extends BookieCommand<NukeExistingClusterCommand.NukeExistingClusterFlags> {

    static final Logger LOG = LoggerFactory.getLogger(NukeExistingClusterCommand.class);

    private static final String NAME = "nukeexistingcluster";
    private static final String DESC = "Nuke bookkeeper cluster by deleting metadata.";

    public NukeExistingClusterCommand() {
        this(new NukeExistingClusterFlags());
    }

    private NukeExistingClusterCommand(NukeExistingClusterFlags flags) {
        super(CliSpec.<NukeExistingClusterCommand.NukeExistingClusterFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    /**
     * Flags for nuke existing cluster command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class NukeExistingClusterFlags extends CliFlags {

        @Parameter(names = {"-f", "--force"},
            description = "If instance id is not specified, then whether to force nuke "
                          + "the metadata without " + "validating instance id")
        private boolean force;

        @Parameter(names = {"-p", "--zkledgersrootpath"}, description = "zookeeper ledgers root path", required = true)
        private String zkLedgersRootPath;

        @Parameter(names = {"-i", "--instanceid"}, description = "instance id")
        private String instandId;

    }

    @Override
    public boolean apply(ServerConfiguration conf, NukeExistingClusterFlags cmdFlags) {
        /*
         * for NukeExistingCluster command 'zkledgersrootpath' should be provided and either force option or
         * instanceid should be provided.
         */
        if (cmdFlags.force == (cmdFlags.instandId != null)) {
            LOG.error("Either force option or instanceid should be specified (but no both)");
            return false;
        }
        try {
            return BookKeeperAdmin.nukeExistingCluster(conf, cmdFlags.zkLedgersRootPath,
                                                         cmdFlags.instandId, cmdFlags.force);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
