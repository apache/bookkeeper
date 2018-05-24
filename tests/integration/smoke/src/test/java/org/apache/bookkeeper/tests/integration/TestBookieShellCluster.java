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

import static org.junit.Assert.assertTrue;

import com.github.dockerjava.api.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Test cluster related commands in bookie shell.
 */
@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestBookieShellCluster extends BookieShellTestBase {

    @ArquillianResource
    private DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Test
    @Override
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));
    }

    @Test
    @Override
    public void test999_Teardown() {
        assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    @Override
    protected String runCommandInAnyContainer(String... cmds) throws Exception {
        String bookie = BookKeeperClusterUtils.getAnyBookie();
        return DockerUtils.runCommand(docker, bookie, cmds);
    }

    @Test
    @Override
    public void test001_SimpleTest() throws Exception {
        super.test001_SimpleTest();
    }

    @Test
    @Override
    public void test002_ListROBookies() throws Exception {
        super.test002_ListROBookies();
    }

    @Test
    @Override
    public void test003_ListRWBookies() throws Exception {
        super.test003_ListRWBookies();
    }
}
