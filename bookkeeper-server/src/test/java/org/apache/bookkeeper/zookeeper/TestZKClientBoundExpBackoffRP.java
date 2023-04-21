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
package org.apache.bookkeeper.zookeeper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test zk client resiliency with BoundExponentialBackoffRetryPolicy.
 */
@RunWith(Parameterized.class)
public class TestZKClientBoundExpBackoffRP extends TestZooKeeperClient {

    public TestZKClientBoundExpBackoffRP(Class<? extends ZooKeeperCluster> zooKeeperUtilClass,
                                         Class<? extends RetryPolicy> retryPolicyClass)
            throws IOException, KeeperException, InterruptedException {
        super(zooKeeperUtilClass, retryPolicyClass);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> zooKeeperUtilClass() {
        return Arrays.asList(new Object[][] { { ZooKeeperUtil.class, BoundExponentialBackoffRetryPolicy.class },
                { ZooKeeperClusterUtil.class, BoundExponentialBackoffRetryPolicy.class } });
    }

}