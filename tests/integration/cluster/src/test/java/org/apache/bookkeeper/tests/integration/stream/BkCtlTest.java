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
package org.apache.bookkeeper.tests.integration.stream;

import static org.junit.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.containers.BookieContainer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testcontainers.containers.Container.ExecResult;

/**
 * Integration test for `bkctl`.
 */
@Slf4j
public class BkCtlTest extends StreamClusterTestBase {

    @Rule
    public final TestName testName = new TestName();

    //
    // `bookies` commands
    //

    @Test
    public void listBookies() throws Exception {
        BookieContainer bookie = bkCluster.getAnyBookie();
        ExecResult result = bookie.execCmd(
            BKCTL,
            "bookies",
            "list"
        );
        String output = result.getStdout();
        assertTrue(output.contains("ReadWrite Bookies :"));
    }

    //
    // `bookie` commands
    //

    @Test
    public void showLastMark() throws Exception {
        BookieContainer bookie = bkCluster.getAnyBookie();
        ExecResult result = bookie.execCmd(
            BKCTL,
            "bookie",
            "lastmark"
        );
        assertTrue(result.getStdout().contains("LastLogMark : Journal"));
    }

    //
    // `ledger` commands
    //

    @Test
    public void simpleTest() throws Exception {
        ExecResult result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            "ledger",
            "simpletest",
            "--ensemble-size", "3",
            "--write-quorum-size", "3",
            "--ack-quorum-size", "2",
            "--num-entries", "100"
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
        ExecResult result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "namespace",
            "create",
            nsName
        );
        assertTrue(
            result.getStdout().contains("Successfully created namespace '" + nsName + "'"));
    }

    //
    // `tables` commands
    //

    @Test
    public void createTable() throws Exception {
        String tableName = testName.getMethodName();
        ExecResult result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "tables",
            "create",
            tableName
        );
        assertTrue(
            result.getStdout().contains("Successfully created table '" + tableName + "'"));
    }

    //
    // `table` commands
    //

    @Test
    public void putGetKey() throws Exception {
        String key = testName.getMethodName() + "-key";
        String value = testName.getMethodName() + "-value";
        ExecResult result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "table",
            "put",
            TEST_TABLE,
            key,
            value
        );
        assertTrue(
            result.getStdout().contains(String.format("Successfully update kv: ('%s', '%s')",
                key, value
                )));

        result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "table",
            "get",
            TEST_TABLE,
            key);
        assertTrue(
            result.getStdout().contains(String.format("value = %s", value)));
    }

    @Test
    public void incGetKey() throws Exception {
        String key = testName.getMethodName() + "-key";
        long value = System.currentTimeMillis();
        ExecResult result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "table",
            "inc",
            TEST_TABLE,
            key,
            "" + value
        );
        assertTrue(
            result.getStdout().contains(String.format("Successfully increment kv: ('%s', amount = '%s').",
                key, value
                )));

        result = bkCluster.getAnyBookie().execCmd(
            BKCTL,
            STREAM_URI,
            "table",
            "get",
            TEST_TABLE,
            key);
        assertTrue(
            result.getStdout().contains(String.format("value = %s", value)));
    }

}
