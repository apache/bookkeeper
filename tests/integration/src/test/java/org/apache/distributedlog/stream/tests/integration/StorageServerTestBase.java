/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.tests.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.distributedlog.stream.cluster.StreamCluster;
import org.apache.distributedlog.stream.cluster.StreamClusterSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


/**
 * Test Base for Range Server related tests.
 */
@Slf4j
public abstract class StorageServerTestBase {

  static {
    // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
    // are disabled by default due to security reasons
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
  }

  @Rule
  public final TemporaryFolder testDir = new TemporaryFolder();

  protected final StreamClusterSpec spec;
  protected StreamCluster cluster;

  protected StorageServerTestBase() {
    this(StreamClusterSpec.builder()
      .baseConf(new CompositeConfiguration())
      .numServers(3)
      .build());
  }

  protected StorageServerTestBase(StreamClusterSpec spec) {
    this.spec = spec;
  }

  @Before
  public void setUp() throws Exception {
    spec.storageRootDir(testDir.newFolder("tests"));
    this.cluster = StreamCluster.build(spec);
    this.cluster.start();
    doSetup();
  }

  protected abstract void doSetup() throws Exception;

  @After
  public void tearDown() throws Exception {
    doTeardown();
    if (null != this.cluster) {
      this.cluster.stop();
    }
  }

  protected abstract void doTeardown() throws Exception;

}
