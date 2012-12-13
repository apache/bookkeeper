/*
 *
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
 *
 */
package org.apache.hedwig.server.meta;

import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.ZkMetadataManagerFactory;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public abstract class MetadataManagerFactoryTestCase extends ZooKeeperTestBase {
    static Logger LOG = LoggerFactory.getLogger(MetadataManagerFactoryTestCase.class);

    protected MetadataManagerFactory metadataManagerFactory;
    protected ServerConfiguration conf;

    public MetadataManagerFactoryTestCase(String metadataManagerFactoryCls) {
        super();
        conf = new ServerConfiguration();
        conf.setMetadataManagerFactoryName(metadataManagerFactoryCls);
        conf.getConf().setProperty("metastore_impl_class", InMemoryMetaStore.class.getName());
        InMemoryMetaStore.reset();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { ZkMetadataManagerFactory.class.getName() },
            { MsMetadataManagerFactory.class.getName() },
        });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        metadataManagerFactory = MetadataManagerFactory.newMetadataManagerFactory(conf, zk);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        metadataManagerFactory.shutdown();
        super.tearDown();
    }

}
