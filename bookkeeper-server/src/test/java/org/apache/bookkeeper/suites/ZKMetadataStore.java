/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.suites;

import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.test.ZooKeeperUtil;

/**
 * Start the zookeeper based metadata store.
 */
class ZKMetadataStore implements MetadataStore {

    private final ZooKeeperUtil zkUtil;

    ZKMetadataStore() {
        this.zkUtil = new ZooKeeperUtil();
    }

    @Override
    public void start() throws Exception {
        this.zkUtil.startCluster();
    }

    @Override
    public void close() throws Exception {
        this.zkUtil.killCluster();
    }

    @Override
    public ServiceURI getServiceUri() {
        return ServiceURI.create(zkUtil.getMetadataServiceUri());
    }
}
