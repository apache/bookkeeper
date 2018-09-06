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
package org.apache.bookkeeper.tests.integration.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.DockerClient;

import java.io.IOException;
import java.net.Socket;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.Cleanup;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for interacting a bookkeeper cluster used for integration tests.
 */
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
        return zookeeperRunning(ip, 2181);
    }
    public static boolean zookeeperRunning(String ip, int port) {
        try (Socket socket = new Socket(ip, port)) {
            socket.setSoTimeout(1000);
            socket.getOutputStream().write("ruok".getBytes(UTF_8));
            byte[] resp = new byte[4];
            if (socket.getInputStream().read(resp) == 4) {
                return new String(resp, UTF_8).equals("imok");
            }
        } catch (IOException e) {
            // ignore, we'll return fallthrough to return false
        }
        return false;
    }

    public static void legacyMetadataFormat(DockerClient docker) throws Exception {
        @Cleanup
        ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker);
        zk.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static boolean metadataFormatIfNeeded(DockerClient docker, String version) throws Exception {
        @Cleanup
        ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker);
        if (zk.exists("/ledgers", false) == null) {
            String bookkeeper = "/opt/bookkeeper/" + version + "/bin/bookkeeper";
            runOnAnyBookie(docker, bookkeeper, "shell", "metaformat", "-nonInteractive");
            return true;
        } else {
            return false;
        }
    }

    public static String createDlogNamespaceIfNeeded(DockerClient docker,
                                                     String version,
                                                     String namespace) throws Exception {
        String zkServers = BookKeeperClusterUtils.zookeeperConnectString(docker);
        String dlogUri = "distributedlog://" + zkServers + namespace;

        @Cleanup
        ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker);
        if (zk.exists(namespace, false) == null) {
            String dlog = "/opt/bookkeeper/" + version + "/bin/dlog";

            runOnAnyBookie(docker, dlog,
                "admin",
                "bind",
                "-l", "/ledgers",
                "-s", zkServers,
                "-c", dlogUri);
        }
        return dlogUri;
    }

    public static void formatAllBookies(DockerClient docker, String version) throws Exception {
        String bookkeeper = "/opt/bookkeeper/" + version + "/bin/bookkeeper";
        BookKeeperClusterUtils.runOnAllBookies(docker, bookkeeper, "shell", "bookieformat", "-nonInteractive");
    }

    public static void updateBookieConf(DockerClient docker, String containerId,
                                        String version, String key, String value) throws Exception {
        String confFile = "/opt/bookkeeper/" + version + "/conf/bk_server.conf";
        String sedProgram = String.format(
                "/[[:blank:]]*%s[[:blank:]]*=/ { h; s!=.*!=%s!; }; ${x;/^$/ { s//%s=%s/;H; }; x}",
                key, value, key, value);
        DockerUtils.runCommand(docker, containerId, "sed", "-i", "-e", sedProgram, confFile);
    }

    public static void updateAllBookieConf(DockerClient docker, String version, String key, String value)
            throws Exception {
        for (String b : allBookies()) {
            updateBookieConf(docker, b, version, key, value);
        }
    }

    public static boolean runOnAnyBookie(DockerClient docker, String... cmds) throws Exception {
        Optional<String> bookie = allBookies().stream().findAny();
        if (bookie.isPresent()) {
            DockerUtils.runCommand(docker, bookie.get(), cmds);
            return true;
        } else {
            return false;
        }
    }

    public static String getAnyBookie() throws Exception {
        Optional<String> bookie = allBookies().stream().findAny();
        if (bookie.isPresent()) {
            return bookie.get();
        } else {
            throw new Exception("No bookie is available");
        }
    }

    public static void runOnAllBookies(DockerClient docker, String... cmds) throws Exception {
        for (String b : allBookies()) {
            DockerUtils.runCommand(docker, b, cmds);
        }
    }

    public static Set<String> allBookies() {
        return DockerUtils.cubeIdsMatching("bookkeeper");
    }

    private static boolean waitBookieState(DockerClient docker, String containerId,
                                           int timeout, TimeUnit timeoutUnit,
                                           boolean upOrDown) {
        long timeoutMillis = timeoutUnit.toMillis(timeout);
        long pollMillis = 1000;
        String bookieId = DockerUtils.getContainerIP(docker, containerId) + ":3181";
        try {
            @Cleanup
            ZooKeeper zk = BookKeeperClusterUtils.zookeeperClient(docker);
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
        LOG.warn("Bookie {} didn't go {} after {} seconds",
                 containerId, upOrDown ? "up" : "down",
                 timeoutUnit.toSeconds(timeout));
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

    private static boolean allTrue(boolean accumulator, boolean result) {
        return accumulator && result;
    }

    public static boolean startAllBookiesWithVersion(DockerClient docker, String version)
            throws Exception {
        return allBookies().stream()
            .map((b) -> startBookieWithVersion(docker, b, version))
            .reduce(true, BookKeeperClusterUtils::allTrue);
    }

    public static boolean stopBookie(DockerClient docker, String containerId) {
        try {
            DockerUtils.runCommand(docker, containerId, "supervisorctl", "stop", "all");
        } catch (Exception e) {
            LOG.error("Exception stopping bookie", e);
            return false;
        }
        return waitBookieDown(docker, containerId, 5, TimeUnit.SECONDS);
    }

    public static boolean stopAllBookies(DockerClient docker) {
        return allBookies().stream()
            .map((b) -> stopBookie(docker, b))
            .reduce(true, BookKeeperClusterUtils::allTrue);
    }

    public static boolean waitAllBookieUp(DockerClient docker) {
        return allBookies().stream()
            .map((b) -> waitBookieUp(docker, b, 10, TimeUnit.SECONDS))
            .reduce(true, BookKeeperClusterUtils::allTrue);
    }
}
