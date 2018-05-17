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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.ContainerNetwork;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);

    private static File getTargetDirectory(String containerId) {
        String base = System.getProperty("maven.buildDirectory");
        if (base == null) {
            base = "target";
        }
        File directory = new File(base + "/container-logs/" + containerId);
        if (!directory.exists() && !directory.mkdirs()) {
            LOG.error("Error creating directory for container logs.");
        }
        return directory;
    }

    public static void dumpContainerLogToTarget(DockerClient docker, String containerId) {
        File output = new File(getTargetDirectory(containerId), "docker.log");
        try (FileOutputStream os = new FileOutputStream(output)) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            docker.logContainerCmd(containerId).withStdOut(true)
                .withStdErr(true).withTimestamps(true).exec(new ResultCallback<Frame>() {
                        @Override
                        public void close() {}

                        @Override
                        public void onStart(Closeable closeable) {}

                        @Override
                        public void onNext(Frame object) {
                            try {
                                os.write(object.getPayload());
                            } catch (IOException e) {
                                onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }

                        @Override
                        public void onComplete() {
                            future.complete(true);
                        }
                    });
            future.get();
        } catch (RuntimeException|ExecutionException|IOException e) {
            LOG.error("Error dumping log for {}", containerId, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted dumping log from container {}", containerId, ie);
        }
    }

    public static void dumpContainerLogDirToTarget(DockerClient docker, String containerId, String path) {
        final int READ_BLOCK_SIZE = 10000;

        try (InputStream dockerStream = docker.copyArchiveFromContainerCmd(containerId, path).exec();
             TarArchiveInputStream stream = new TarArchiveInputStream(dockerStream)) {
            TarArchiveEntry entry = stream.getNextTarEntry();
            while (entry != null) {
                if (entry.isFile()) {
                    File output = new File(getTargetDirectory(containerId), entry.getName().replace("/", "-"));
                    try (FileOutputStream os = new FileOutputStream(output)) {
                        byte[] block = new byte[READ_BLOCK_SIZE];
                        int read = stream.read(block, 0, READ_BLOCK_SIZE);
                        while (read > -1) {
                            os.write(block, 0, read);
                            read = stream.read(block, 0, READ_BLOCK_SIZE);
                        }
                    }
                }
                entry = stream.getNextTarEntry();
            }
        } catch (RuntimeException|IOException e) {
            LOG.error("Error reading bk logs from container {}", containerId, e);
        }
    }

    public static String getContainerIP(DockerClient docker, String containerId) {
        for (Map.Entry<String, ContainerNetwork> e : docker.inspectContainerCmd(containerId)
                 .exec().getNetworkSettings().getNetworks().entrySet()) {
            return e.getValue().getIpAddress();
        }
        throw new IllegalArgumentException("Container " + containerId + " has no networks");
    }

    public static String runCommand(DockerClient docker, String containerId, String... cmd) throws Exception {
        return runCommand(docker, containerId, false, cmd);
    }

    public static String runCommand(DockerClient docker, String containerId, boolean ignoreError, String... cmd)
            throws Exception {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        String execid = docker.execCreateCmd(containerId)
            .withCmd(cmd)
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec()
            .getId();
        String cmdString = Arrays.stream(cmd).collect(Collectors.joining(" "));
        StringBuffer output = new StringBuffer();
        docker.execStartCmd(execid).withDetach(false).exec(new ResultCallback<Frame>() {
                @Override
                public void close() {}

                @Override
                public void onStart(Closeable closeable) {
                    LOG.info("DOCKER.exec({}:{}): Executing...", containerId, cmdString);
                }

                @Override
                public void onNext(Frame object) {
                    LOG.info("DOCKER.exec({}:{}): {}", containerId, cmdString, object);
                    output.append(new String(object.getPayload(), UTF_8));
                }

                @Override
                public void onError(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    LOG.info("DOCKER.exec({}:{}): Done", containerId, cmdString);
                    future.complete(true);
                }
            });
        future.get();

        InspectExecResponse resp = docker.inspectExecCmd(execid).exec();
        while (resp.isRunning()) {
            Thread.sleep(200);
            resp = docker.inspectExecCmd(execid).exec();
        }
        int retCode = resp.getExitCode();
        if (retCode != 0) {
            LOG.error("DOCKER.exec({}:{}): failed with {} : {}", containerId, cmdString, retCode, output);
            if (!ignoreError) {
                throw new Exception(String.format("cmd(%s) failed on %s with exitcode %d",
                    cmdString, containerId, retCode));
            }
        } else {
            LOG.info("DOCKER.exec({}:{}): completed with {}", containerId, cmdString, retCode);
        }
        return output.toString();
    }

    public static Set<String> cubeIdsMatching(String needle) {
        Pattern pattern = Pattern.compile("^arq.cube.docker.([^.]*).ip$");
        return System.getProperties().keySet().stream()
            .map(k -> pattern.matcher(k.toString()))
            .filter(m -> m.matches())
            .map(m -> m.group(1))
            .filter(m -> m.contains(needle))
            .collect(Collectors.toSet());
    }
}
