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

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to print instance id of the cluster.
 */
public class InstanceIdCommand extends BookieCommand<CliFlags> {

    static final Logger LOG = LoggerFactory.getLogger(InstanceIdCommand.class);

    private static final String NAME = "instanceid";
    private static final String DESC = "Print the instanceid of the cluster";

    public InstanceIdCommand() {
        super(CliSpec.newBuilder().withName(NAME).withDescription(DESC).withFlags(new CliFlags()).build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        try {
            runFunctionWithRegistrationManager(conf, rm -> {
                String readInstanceId = null;
                try {
                    readInstanceId = rm.getClusterInstanceId();
                } catch (BookieException e) {
                    throw new UncheckedExecutionException(e);
                }
                LOG.info("Metadata Service Uri: {} InstanceId: {}",
                         conf.getMetadataServiceUriUnchecked(), readInstanceId);
                return null;
            });
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
        return true;
    }
}
