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

import static org.apache.bookkeeper.tools.cli.helpers.CommandHelpers.getBookieSocketAddrStringRepresentation;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.net.URI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to print which node has the auditor lock.
 */
public class WhoIsAuditorCommand extends BookieCommand<CliFlags> {

    static final Logger LOG = LoggerFactory.getLogger(WhoIsAuditorCommand.class);

    private static final String NAME = "whoisauditor";
    private static final String DESC = "Print the node which holds the auditor lock.";

    public WhoIsAuditorCommand() {
        super(CliSpec.newBuilder()
                     .withName(NAME)
                     .withDescription(DESC)
                     .withFlags(new CliFlags())
                     .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
        try {
            return getAuditor(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    public boolean getAuditor(ServerConfiguration conf)
        throws ConfigurationException, InterruptedException, IOException, KeeperException {
        ZooKeeper zk = null;
        try {
            String metadataServiceUri = conf.getMetadataServiceUri();
            String zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(URI.create(metadataServiceUri));
            zk = ZooKeeperClient.newBuilder()
                                .connectString(zkServers)
                                .sessionTimeoutMs(conf.getZkTimeout())
                                .build();
            BookieSocketAddress bookieId = AuditorElector.getCurrentAuditor(conf, zk);
            if (bookieId == null) {
                LOG.info("No auditor elected");
                return false;
            }
            LOG.info("Auditor: " + getBookieSocketAddrStringRepresentation(bookieId));
        } finally {
            if (zk != null) {
                zk.close();
            }
        }
        return true;
    }
}
