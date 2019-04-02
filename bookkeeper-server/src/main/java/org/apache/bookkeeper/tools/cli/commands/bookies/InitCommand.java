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

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * Intializes new cluster by creating required znodes for the cluster. If
 * ledgersrootpath is already existing then it will error out. If for any
 * reason it errors out while creating znodes for the cluster, then before
 * running initnewcluster again, try nuking existing cluster by running
 * nukeexistingcluster. This is required because ledgersrootpath znode would
 * be created after verifying that it doesn't exist, hence during next retry
 * of initnewcluster it would complain saying that ledgersrootpath is
 * already existing.
 */
public class InitCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "init";
    private static final String DESC =
        "Initializes a new bookkeeper cluster. If initnewcluster fails then try nuking "
        + "existing cluster by running nukeexistingcluster before running initnewcluster again";

    public InitCommand() {
        super(CliSpec.newBuilder()
                     .withName(NAME)
                     .withDescription(DESC)
                     .withFlags(new CliFlags())
                     .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        try {
            return BookKeeperAdmin.initNewCluster(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }
}
