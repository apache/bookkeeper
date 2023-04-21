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

package org.apache.bookkeeper.tests.integration.standalone;

import static org.junit.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.containers.BKStandaloneContainer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testcontainers.containers.Container.ExecResult;

/**
 * A simple test to cover running current docker image in standalone mode.
 */
@Slf4j
public class StandaloneTest {

    @Rule
    public final TestName testName = new TestName();

    @ClassRule
    public static BKStandaloneContainer bkContainer = new BKStandaloneContainer<>("integrationtest", 3);

    @Test
    public void runSimpleTest() throws Exception {
        ExecResult result = bkContainer.execCmd(
            "/opt/bookkeeper/bin/bookkeeper",
            "shell",
            "simpletest",
            "-ensemble", "3",
            "-writeQuorum", "3",
            "-ackQuorum", "2",
            "-numEntries", "100"
        );
        assertTrue(
            result.getStdout().contains("100 entries written to ledger"));
    }

    //
    // `namespace` commands
    //

    @Test
    public void createNamespace() throws Exception {
        String nsName = testName.getMethodName();
        ExecResult result = bkContainer.execCmd(
            "/opt/bookkeeper/bin/bkctl",
            "-u bk://localhost:4181",
            "namespace",
            "create",
            nsName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created namespace '" + nsName + "'"));
    }

    //
    // `tables` commands
    //

    @Test
    public void createTable() throws Exception {
        createNamespace();
        String tableName = testName.getMethodName();
        ExecResult result = bkContainer.execCmd(
            "/opt/bookkeeper/bin/bkctl",
            "-u bk://localhost:4181",
            "--namespace",
            testName.getMethodName(),
            "tables",
            "create",
            tableName
        );
        assertTrue(
            result.getStdout(),
            result.getStdout().contains("Successfully created table '" + tableName + "'"));
    }

}
