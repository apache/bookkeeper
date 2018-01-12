/**
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
package org.apache.bookkeeper.tests;

import com.github.dockerjava.api.DockerClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.net.Socket;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookKeeperClusterUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterUtils.class);

    public static String zookeeperConnectString(DockerClient docker) {
        return DockerUtils.cubeIdsMatching("zookeeper").stream()
            .map((id) -> DockerUtils.getContainerIP(docker, id)).collect(Collectors.joining(":"));
    }

    public static ZooKeeper zookeeperClient(DockerClient docker) throws Exception {
        String connectString = BookKeeperClusterUtils.zookeeperConnectString(docker);
        CompletableFuture<Void> future = new CompletableFuture<>();
        ZooKeeper zk = new ZooKeeper(connectString, 10000,
                                     (e) -> {
                                         if (e.getState().equals(KeeperState.SyncConnected)) {
                                             future.complete(null);
                                         }
                                     });
        future.get();
        return zk;
    }

    public static boolean zookeeperRunning(DockerClient docker, String containerId) {
        String ip = DockerUtils.getContainerIP(docker, containerId);
        try (Socket socket = new Socket(ip, 2181)) {
            socket.setSoTimeout(1000);
            socket.getOutputStream().write("ruok".getBytes());
            byte[] resp = new byte[4];
            socket.getInputStream().read(resp);
            return new String(resp).equals("imok");
        } catch (Exception e) {
            // ignore, we'll return fallthrough to return false
        }
        return false;
    }

    public static void legacyMetadataFormat(DockerClient docker) throws Exception {
        try (ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker)) {
            zk.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private static boolean waitBookieState(DockerClient docker, String containerId,
                                          int timeout, TimeUnit timeoutUnit,
                                          boolean upOrDown) {
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 1000;
        String bookieId = DockerUtils.getContainerIP(docker, containerId) + ":3181";
        try (ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker)) {
            String path = "/ledgers/available/" + bookieId;
            while (timeoutMillis > 0) {
                if ((zk.exists(path, false) != null) == upOrDown) {
                    return true;
                }
                Thread.sleep(pollMillis);
                timeoutMillis -= pollMillis;
            }
        } catch (Exception e) {
            LOG.error("Exception checking for bookie state", e);
            return false;
        }
        return false;
    }

    public static boolean waitBookieUp(DockerClient docker, String containerId,
                                       int timeout, TimeUnit timeoutUnit) {
        return waitBookieState(docker, containerId, timeout, timeoutUnit, true);
    }

    public static boolean waitBookieDown(DockerClient docker, String containerId,
                                         int timeout, TimeUnit timeoutUnit) {
        return waitBookieState(docker, containerId, timeout, timeoutUnit, false);
    }

    public static boolean startBookieWithVersion(DockerClient docker, String containerId, String version) {
        try {
            DockerUtils.runCommand(docker, containerId, "supervisorctl", "start", "bookkeeper-" + version);
        } catch (Exception e) {
            LOG.error("Exception starting bookie", e);
            return false;
        }
        return waitBookieUp(docker, containerId, 10, TimeUnit.SECONDS);
    }

    public static boolean startAllBookiesWithVersion(DockerClient docker, String version)
            throws Exception {
        return DockerUtils.cubeIdsMatching("bookkeeper").stream()
            .map((b) -> startBookieWithVersion(docker, b, version))
            .reduce(true, (accumulator, result) -> Boolean.valueOf(accumulator) && Boolean.valueOf(result));
    }

    public static boolean stopBookie(DockerClient docker, String containerId) {
        try {
            DockerUtils.runCommand(docker, containerId, "supervisorctl", "stop", "all");
        } catch (Exception e) {
            LOG.error("Exception stopping bookie", e);
            return false;
        }
        return waitBookieDown(docker, containerId, 10, TimeUnit.SECONDS);
    }

    public static boolean stopAllBookies(DockerClient docker) {
        return DockerUtils.cubeIdsMatching("bookkeeper").stream()
            .map((b) -> stopBookie(docker, b))
            .reduce(true, (accumulator, result) -> Boolean.valueOf(accumulator) && Boolean.valueOf(result));
    }
}
