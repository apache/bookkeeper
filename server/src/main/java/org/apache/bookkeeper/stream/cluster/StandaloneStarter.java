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

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;

/**
 * A starter to start a standalone cluster.
 */
@Slf4j
public class StandaloneStarter {

    public static void main(String[] args) {
        int retCode = doMain(args);

        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {
        StreamClusterSpec spec = StreamClusterSpec.builder()
            .numServers(3)
            .initialBookiePort(3181)
            .initialGrpcPort(4181)
            .serveReadOnlyTable(true)
            .shouldStartZooKeeper(true)
            .storageRootDir(Paths.get("data", "streamstorage").toFile())
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
