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
package org.apache.bookkeeper.stream.cluster;

import static com.google.common.base.Preconditions.checkArgument;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * A starter to start a standalone cluster.
 */
@Slf4j
public class StandaloneStarter {

    /**
     * Args for standalone starter.
     */
    private static class StarterArgs {

        @Parameter(
            names = {
                "-c", "--conf"
            },
            description = "Configuration file path")
        String configFile = null;

        @Parameter(
            names = {
                "--num-bookies"
            },
            description = "Num of bookies")
        int numBookies = 1;

        @Parameter(
            names = {
                "--zk-port"
            },
            description = "ZooKeeper port")
        int zkPort = 2181;

        @Parameter(
            names = {
                "-u", "--metadata-service-uri"
            },
            description = "Uri of the external metadata service."
                + " If set, the standalone will not start any metadata service itself.")
        String metadataServiceUri = null;

        @Parameter(
            names = {
                "--initial-bookie-port"
            },
            description = "Initial bookie port")
        int initialBookiePort = 3181;

        @Parameter(
            names = {
                "--initial-bookie-grpc-port"
            },
            description = "Initial bookie grpc port")
        int initialBookieGrpcPort = 4181;

        @Parameter(
            names = {
                "--data-dir"
            },
            description = "Location to store standalone data"
        )
        String dataDir = "data";

        @Parameter(
            names = {
                "--wipe-data"
            },
            description = "Clean up previous standalone data")
        boolean wipeData = false;

        @Parameter(
            names = {
                "-h", "--help"
            },
            description = "Show this help message")
        boolean help = false;

    }

    public static void main(String[] args) throws Exception {
        int retCode = doMain(args);

        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) throws Exception {
        StarterArgs starterArgs = new StarterArgs();

        JCommander commander = new JCommander();
        try {
            commander.setProgramName("standalone-starter");
            commander.addObject(starterArgs);
            commander.parse(args);
            if (starterArgs.help) {
                commander.usage();
                return 0;
            }
        } catch (Exception e) {
            commander.usage();
            return -1;
        }

        StreamClusterSpec.StreamClusterSpecBuilder specBuilder = StreamClusterSpec.builder();
        if (starterArgs.metadataServiceUri == null) {
            specBuilder = specBuilder
                .zkPort(starterArgs.zkPort)
                .shouldStartZooKeeper(true);
        } else {
            ServiceURI serviceURI = ServiceURI.create(starterArgs.metadataServiceUri);
            specBuilder = specBuilder
                .metadataServiceUri(serviceURI)
                .shouldStartZooKeeper(false);
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        if (null != starterArgs.configFile) {
            PropertiesConfiguration propsConf = new PropertiesConfiguration(starterArgs.configFile);
            conf.addConfiguration(propsConf);
        }

        checkArgument(starterArgs.numBookies > 0, "Invalid number of bookies : " + starterArgs.numBookies);
        if (starterArgs.numBookies == 1) {
            conf.setProperty("dlog.bkcEnsembleSize", 1);
            conf.setProperty("dlog.bkcWriteQuorumSize", 1);
            conf.setProperty("dlog.bkcAckQuorumSize", 1);
        } else {
            conf.setProperty("dlog.bkcEnsembleSize", starterArgs.numBookies);
            conf.setProperty("dlog.bkcWriteQuorumSize", starterArgs.numBookies);
            conf.setProperty("dlog.bkcAckQuorumSize", starterArgs.numBookies - 1);
        }

        StreamClusterSpec spec = specBuilder
            .baseConf(conf)
            .numServers(starterArgs.numBookies)
            .initialBookiePort(starterArgs.initialBookiePort)
            .initialGrpcPort(starterArgs.initialBookieGrpcPort)
            .storageRootDir(new File(starterArgs.dataDir))
            .build();

        CountDownLatch liveLatch = new CountDownLatch(1);

        StreamCluster cluster = StreamCluster.build(spec);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cluster.stop();
            cluster.close();
            liveLatch.countDown();
        }, "Standalone-Shutdown-Thread"));

        cluster.start();

        try {
            liveLatch.await();
        } catch (InterruptedException e) {
            log.error("The standalone cluster is interrupted : ", e);
        }
        return 0;
    }

}
