/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.cli.commands.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.stream.cli.Commands.OP_INIT;

import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stream.cli.commands.cluster.InitClusterCommand.Flags;
import org.apache.bookkeeper.stream.storage.StorageConstants;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.bookkeeper.tools.common.BKCommand;
import org.apache.bookkeeper.tools.common.BKFlags;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * Command to init a cluster.
 */
@Slf4j
public class InitClusterCommand extends BKCommand<Flags> {

    private static final String NAME = OP_INIT;
    private static final String DESC = "Init a cluster";

    /**
     * Flags for the init cluster command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-x", "--cluster-name"
            },
            description = "cluster name. this would be used as a root path for storing metadata.")
        private String clusterName = "";

        @Parameter(
            names = {
                "-l", "--ledgers-path"
            },
            description = "root path to store ledgers' metadata")
        private String ledgersPath = "/ledgers";

        @Parameter(
            names = {
                "-dl", "--dlog-path"
            },
            description = "root path to store dlog metadata")
        private String dlogPath = "/distributedlog";

        @Parameter(
            names = {
                "-n", "--num-storage-containers"
            },
            description = "num of storage containers allocated for stream storage")
        private int numStorageContainers = 32;

    }

    public InitClusterCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .withUsage("bkctl cluster init [flags] <service-uri>")
            .build());
    }

    @Override
    protected boolean acceptServiceUri(ServiceURI serviceURI) {
        // only support zookeeper now
        return ServiceURI.SERVICE_ZK.equals(serviceURI.getServiceName());
    }

    @Override
    protected boolean apply(ServiceURI ignored,
                            CompositeConfiguration conf,
                            BKFlags globalFlags,
                            Flags cmdFlags) {
        checkArgument(
            !cmdFlags.arguments.isEmpty(),
            "No service URI is provided");

        ServiceURI serviceURI = ServiceURI.create(cmdFlags.arguments.get(0));

        if (null != cmdFlags.clusterName) {
            checkArgument(
                !cmdFlags.clusterName.contains("/"),
                "Invalid cluster name : " + cmdFlags.clusterName);
        }

        checkArgument(
            !Strings.isNullOrEmpty(cmdFlags.ledgersPath)
                && cmdFlags.ledgersPath.startsWith("/"),
            "Invalid ledgers root metadata path : " + cmdFlags.ledgersPath);

        checkArgument(
            !Strings.isNullOrEmpty(cmdFlags.dlogPath)
                && cmdFlags.dlogPath.startsWith("/"),
            "Invalid dlog root metadata path : " + cmdFlags.dlogPath);

        checkArgument(
            cmdFlags.numStorageContainers > 0,
            "Zero or negative number of storage containers configured");

        String clusterName = null == cmdFlags.clusterName ? "" : cmdFlags.clusterName;
        String ledgersPath = getFullyQualifiedPath(clusterName, cmdFlags.ledgersPath);
        String dlogPath = getFullyQualifiedPath(clusterName, cmdFlags.dlogPath);

        String metadataServiceHosts = StringUtils.join(serviceURI.getServiceHosts(), ",");

        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
            metadataServiceHosts,
            new BoundedExponentialBackoffRetry(100, 10000, 20)
        )) {
            client.start();

            URI uri = serviceURI.getUri();
            URI rootUri = new URI(
                uri.getScheme(),
                uri.getAuthority(),
                "",
                null,
                null);

            String ledgersUri = rootUri.toString() + ledgersPath;
            String dlogUri = rootUri.toString() + dlogPath;

            log.info("Initializing cluster {} : \n"
                + "\tledgers : path = {}, uri = {}\n"
                + "\tdlog: path = {}, uri = {}\n"
                + "\tstream storage: path = {}, num_storage_containers = {}",
                clusterName,
                ledgersPath, ledgersUri,
                dlogPath, dlogUri,
                StorageConstants.ZK_METADATA_ROOT_PATH, cmdFlags.numStorageContainers);

            // create the cluster root path
            initializeCluster(client, clusterName);

            // init the ledgers metadata
            initLedgersMetadata(ledgersUri);

            // init the dlog metadata
            initDlogMetadata(client, metadataServiceHosts, dlogUri, dlogPath, ledgersPath);

            // init the stream storage metadata
            initStreamStorageMetadata(
                metadataServiceHosts,
                ledgersUri,
                cmdFlags.numStorageContainers);
            log.info("Successfully initialized cluster {}", clusterName);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeCluster(CuratorFramework client,
                                   String clusterName) throws Exception {
        if (Strings.isNullOrEmpty(clusterName)) {
            return;
        }

        String path = "/" + clusterName;
        if (null == client.checkExists().forPath(path)) {
            try {
                client.create().forPath(path);
            } catch (KeeperException.NodeExistsException ne) {
                // someone created it already
                return;
            }
        }
    }

    private void initLedgersMetadata(String ledgersUri) throws Exception {
        MetadataDrivers.runFunctionWithRegistrationManager(
            new ServerConfiguration().setMetadataServiceUri(ledgersUri),
            rm -> {
                try {
                    if (rm.initNewCluster()) {
                        log.info("Successfully initialized ledgers metadata at {}", ledgersUri);
                    }
                } catch (Exception e) {
                    throw new UncheckedExecutionException("Failed to init ledgers metadata at " + ledgersUri, e);
                }
                return null;
            }
        );
    }

    private void initDlogMetadata(CuratorFramework client,
                                  String metadataServiceHosts,
                                  String dlogUri,
                                  String dlogPath,
                                  String ledgersPath) throws Exception {
        BKDLConfig dlogConfig = new BKDLConfig(metadataServiceHosts, ledgersPath);
        DLMetadata dlogMetadata = DLMetadata.create(dlogConfig);

        if (null == client.checkExists().forPath(dlogPath)) {
            try {
                dlogMetadata.create(URI.create(dlogUri));
            } catch (ZKException zke) {
                if (Code.NODEEXISTS.intValue() == zke.getCode()) {
                    // dlog namespace already created
                    return;
                }
            }
        }
    }

    private void initStreamStorageMetadata(String metadataServiceHosts,
                                           String ledgersUri,
                                           int numStorageContainers) {
        ZkClusterInitializer initializer = new ZkClusterInitializer(metadataServiceHosts);
        if (initializer.initializeCluster(URI.create(ledgersUri), numStorageContainers)) {
            log.info("Successfully initialized stream storage metadata at {}:{}",
                metadataServiceHosts,
                StorageConstants.ZK_METADATA_ROOT_PATH);
        }
    }

    private static String getFullyQualifiedPath(String clusterName, String path) {
        if (Strings.isNullOrEmpty(clusterName)) {
            clusterName = "";
        } else {
            clusterName = "/" + clusterName;
        }
        return clusterName + path;
    }

}
