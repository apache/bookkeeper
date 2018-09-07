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

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Base for testing bookie shell scripts.
 */
@Slf4j
public abstract class BookieShellTestBase {

    String currentVersion = System.getProperty("currentVersion");
    String bkScript;

    @Before
    public void setup() {
        bkScript = "/opt/bookkeeper/" + currentVersion + "/bin/bookkeeper";
    }

    @Test
    public abstract void test000_Setup() throws Exception;

    @Test
    public abstract void test999_Teardown();

    protected abstract String runCommandInAnyContainer(String... cmd) throws Exception;

    @Test
    public void test001_SimpleTest() throws Exception {
        assertTrue(runCommandInAnyContainer(
            bkScript,
            "shell",
            "simpletest",
            "-ensemble", "3",
            "-writeQuorum", "3",
            "-ackQuorum", "2",
            "-numEntries", "100"
        ).contains("100 entries written to ledger"));
    }

    @Test
    public void test002_ListROBookies() throws Exception {
        assertTrue(runCommandInAnyContainer(
            bkScript,
            "shell",
            "listbookies",
            "-ro"
        ).contains("No bookie exists!"));
    }

    @Test
    public void test003_ListRWBookies() throws Exception {
        assertTrue(runCommandInAnyContainer(
            bkScript,
            "shell",
            "listbookies",
            "-rw"
        ).contains("ReadWrite Bookies :"));
    }

}
