/**
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
package org.apache.bookkeeper.net;

import com.google.common.annotations.Beta;

import java.util.List;

/**
 * An interface that must be implemented to allow pluggable
 * DNS-name/IP-address to RackID resolvers.
 *
 */
@Beta
public interface DNSToSwitchMapping {
    /**
     * Resolves a list of DNS-names/IP-addresses and returns back a list of
     * switch information (network paths). One-to-one correspondence must be
     * maintained between the elements in the lists.
     * Consider an element in the argument list - x.y.com. The switch information
     * that is returned must be a network path of the form /foo/rack,
     * where / is the root, and 'foo' is the switch where 'rack' is connected.
     * Note the hostname/ip-address is not part of the returned path.
     * The network topology of the cluster would determine the number of
     * components in the network path.
     *
     * <p>If a name cannot be resolved to a rack, the implementation
     * should return {@link NetworkTopology#DEFAULT_REGION_AND_RACK}. This
     * is what the bundled implementations do, though it is not a formal requirement
     *
     * @param names the list of hosts to resolve (can be empty)
     * @return list of resolved network paths.
     * If <i>names</i> is empty, the returned list is also empty
     */
    List<String> resolve(List<String> names);

    /**
     * Reload all of the cached mappings.
     *
     * <p>If there is a cache, this method will clear it, so that future accesses
     * will get a chance to see the new data.
     */
    void reloadCachedMappings();

    /**
     * Hints what to use with implementation when InetSocketAddress is converted
     * to String:
     * hostname (addr.getHostName(), default)
     * or IP address (addr.getAddress().getHostAddress()).
     * @return true if hostname, false if IP address. Default is true.
      */
    default boolean useHostName() {
        return true;
    }
}
