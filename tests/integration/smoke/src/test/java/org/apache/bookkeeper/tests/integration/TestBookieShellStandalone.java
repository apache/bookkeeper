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

import com.github.dockerjava.api.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

/**
 * Test bookie shell with a standalone cluster.
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestBookieShellStandalone extends BookieShellTestBase {

    private DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");
    private String bookie = "bookkeeper1";
    private String bkBash;

    @Before
    public void setup() {
        bkBash = "/opt/bookkeeper/" + currentVersion + "/bin/bookkeeper";
    }

    @Override
    public void test000_Setup() throws Exception {

    }

    @Override
    public void test999_Teardown() {

    }

    @Override
    protected String runCommandInAnyContainer(String... cmd) throws Exception {
        return null;
    }
}
