/**
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
package org.apache.bookkeeper.tests;

import org.junit.Assert;
import org.junit.Test;

public class MavenClassLoaderTest {
    @Test(expected=ClassNotFoundException.class)
    public void testLog4JReplacement() throws Exception {
        MavenClassLoader.forBookKeeperVersion("4.0.0")
            .newInstance("org.apache.log4j.Logger");
    }

    @Test
    public void testNoZooKeeperInterference() throws Exception {
        // Use KeeperException, because ZooKeeper needs a watcher which would be a pain to construct
        Object o = MavenClassLoader.forBookKeeperVersion("4.0.0")
            .newInstance("org.apache.zookeeper.KeeperException$NoNodeException");
        Assert.assertFalse(o.getClass().equals(
                                   org.apache.zookeeper.KeeperException.NoNodeException.class));
    }

}
