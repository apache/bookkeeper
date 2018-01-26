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

package org.apache.distributedlog.fs;

import java.net.URI;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Integration test for {@link DLFileSystem}.
 */
public abstract class TestDLFSBase extends TestDistributedLogBase {

    @Rule
    public final TestName runtime = new TestName();

    protected static URI dlfsUri;
    protected static DLFileSystem fs;

    @BeforeClass
    public static void setupDLFS() throws Exception {
        setupCluster();
        dlfsUri = DLMTestUtil.createDLMURI(zkPort, "");
        fs = new DLFileSystem();
        Configuration conf = new Configuration();
        conf.set(DLFileSystem.DLFS_CONF_FILE, TestDLFSBase.class.getResource("/dlfs.conf").toURI().getPath());
        fs.initialize(dlfsUri, conf);
        fs.setWorkingDirectory(new Path("/"));
    }

    @AfterClass
    public static void teardownDLFS() throws Exception {
        fs.close();
        teardownCluster();
    }

}
