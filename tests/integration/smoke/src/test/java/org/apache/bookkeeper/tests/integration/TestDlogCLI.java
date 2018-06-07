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

package org.apache.bookkeeper.tests.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.dockerjava.api.DockerClient;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Test `bin/dlog`.
 */
@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDlogCLI {

    private static final String DLOG_STREAM_PREFIX = "stream-";
    private static final String STREAMS_REGEX = "0-99";

    @ArquillianResource
    private DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");
    private String dlogCLI;
    private String dlogUri;

    @Before
    public void setup() {
        dlogCLI = "/opt/bookkeeper/" + currentVersion + "/bin/dlog";
        dlogUri = "distributedlog://" + BookKeeperClusterUtils.zookeeperConnectString(docker) + "/distributedlog";
    }

    @Test
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies, then create a dlog namespace
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        BookKeeperClusterUtils.createDlogNamespaceIfNeeded(docker, currentVersion, "/distributedlog");
    }

    @Test
    public void test999_Teardown() throws Exception {
        assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Test
    public void test001_CreateStreams() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        assertTrue(DockerUtils.runCommand(docker, bookie,
            dlogCLI,
            "tool",
            "create",
            "--prefix", DLOG_STREAM_PREFIX,
            "--expression", STREAMS_REGEX,
            "--uri", dlogUri,
            "-f"
            ).isEmpty());
    }

    @Test
    public void test002_ListStreams() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        String output = DockerUtils.runCommand(docker, bookie,
            dlogCLI,
            "tool",
            "list",
            "--uri", dlogUri,
            "-f"
            );
        String[] lines = output.split("\\r?\\n");
        Set<String> streams = new HashSet<>();
        for (String string : lines) {
            if (string.startsWith(DLOG_STREAM_PREFIX)) {
                streams.add(string);
            }
        }
        assertEquals(100, streams.size());
    }

    @Test
    public void test003_ShowStream() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        String output = DockerUtils.runCommand(docker, bookie, true,
            dlogCLI,
            "tool",
            "show",
            "--uri", dlogUri,
            "--stream", "stream-99",
            "-f");
        assertTrue(output.contains("Log stream-99:<default> has no records"));
    }

    @Test
    public void test004_DeleteStream() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        String output = DockerUtils.runCommand(docker, bookie,
            dlogCLI,
            "tool",
            "delete",
            "--uri", dlogUri,
            "--stream", "stream-99",
            "-f");
        assertTrue(output.isEmpty());
    }

    @Test
    public void test005_CheckStreamDeleted() throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        String output = DockerUtils.runCommand(docker, bookie, true,
            dlogCLI,
            "tool",
            "show",
            "--uri", dlogUri,
            "--stream", "stream-99",
            "-f");
        assertTrue(output.contains("Log stream-99 does not exist or has been deleted"));
    }

}
