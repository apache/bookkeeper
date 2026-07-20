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

package org.apache.bookkeeper.common.net;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.commons.lang3.StringUtils;

/**
 * ServiceURI represents service uri within bookkeeper cluster.
 *
 * <h3>Service URI syntax and components</h3>
 *
 * <p>At the highest level a service uri is a {@link java.net.URI} in string
 * form has the syntax.
 *
 * <blockquote>
 * [<i>service</i>[<i>service-specific-part</i>]<b>{@code :}</b>[<b>{@code //}</b><i>authority</i>][<i>path</i>]
 * </blockquote>
 *
 * <p>where the characters <b>{@code :}</b> and <b>{@code /}</b> stand for themselves.
 *
 * <p>The service-specific-part of a service URI consists of the backend information used for services to use.
 * It has the syntax as below:
 *
 * <blockquote>
 * [({@code -}|{@code +})][<i>backend-part</i>]
 * </blockquote>
 *
 * <p>where the characters <b>{@code -}</b> and <b>{@code +}</b> stand as a separator to separate service type
 * from service backend information.
 *
 * <p>The authority component of a service URI has the same meaning as the authority component
 * in a {@link java.net.URI}. If specified, it should be <i>server-based</i>. A server-based
 * authority parses according to the familiar syntax
 *
 * <blockquote>
 * [<i>user-info</i><b>{@code @}</b>]<i>host</i>[<b>{@code :}</b><i>port</i>]
 * </blockquote>
 *
 * <p>where the characters <b>{@code @}</b> and <b>{@code :}</b> stand for themselves.
 *
 * <p>The path component of a service URI is itself said to be absolute. It typically means which path a service
 * stores metadata or data.
 *
 * <p>All told, then, a service URI instance has the following components:
 *
 * <blockquote>
 * <table summary="Describes the components of a service
 * URI:service,service-specific-part,authority,user-info,host,port,path">
 * <tr><td>service</td><td>{@code String}</td></tr>
 * <tr><td>service-specific-part&nbsp;&nbsp;&nbsp;&nbsp;</td><td>{@code String}</td></tr>
 * <tr><td>authority</td><td>{@code String}</td></tr>
 * <tr><td>user-info</td><td>{@code String}</td></tr>
 * <tr><td>host</td><td>{@code String}</td></tr>
 * <tr><td>port</td><td>{@code int}</td></tr>
 * <tr><td>path</td><td>{@code String}</td></tr>
 * </table>
 * </blockquote>
 *
 * <p>Some examples of service URIs are:
 *
 * <blockquote>
 * {@code zk://localhost:2181/cluster1/ledgers} =&gt; ledger service uri using default ledger manager<br>
 * {@code zk+hierarchical://localhost:2181/ledgers} =&gt; ledger service uri using hierarchical ledger manager<br>
 * {@code etcd://localhost/ledgers} =&gt; ledger service uri using etcd as metadata store<br>
 * {@code distributedlog://localhost:2181/distributedlog} =&gt; distributedlog namespace<br>
 * {@code distributedlog-bk://localhost:2181/distributedlog} =&gt; distributedlog namespace with bk backend<br>
 * {@code bk://bookkeeper-cluster/} =&gt; stream storage service uri <br>
 * {@code host1:port,host2:port} =&gt; a list of hosts as bootstrap hosts to a stream storage cluster}
 * </blockquote>
 *
 * @since 4.8.0
 */
@Public
@Evolving
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@EqualsAndHashCode
public class ServiceURI {

    /**
     * Service string for ledger service that uses zookeeper as metadata store.
     */
    public static final String SERVICE_ZK   = "zk";

    /**
     * Service string for dlog service.
     */
    public static final String SERVICE_DLOG = "distributedlog";

    /**
     * Service string for bookkeeper service.
     */
    public static final String SERVICE_BK = "bk";
    public static final int SERVICE_BK_PORT = 4181;

    /**
     * The default local bk service uri.
     */
    public static final ServiceURI DEFAULT_LOCAL_STREAM_STORAGE_SERVICE_URI =
        ServiceURI.create("bk://localhost:4181");

    private static final String SERVICE_SEP = "+";
    private static final String SERVICE_DLOG_SEP = "-";

    /**
     * Create a service uri instance from a uri string.
     *
     * @param uriStr service uri string
     * @return a service uri instance
     * @throws NullPointerException if {@code uriStr} is null
     * @throws IllegalArgumentException if the given string violates RFC&nbsp;2396
     */
    public static ServiceURI create(String uriStr) {
        checkNotNull(uriStr, "service uri string is null");

        if (uriStr.contains("[") && uriStr.contains("]")) {
            // deal with ipv6 address
            Splitter splitter = Splitter.on(CharMatcher.anyOf(",;"));
            List<String> hosts = splitter.splitToList(uriStr);

            if (hosts.size() > 1) {
                // deal with multi ipv6 hosts
                String firstHost = hosts.get(0);
                String lastHost = hosts.get(hosts.size() - 1);
                boolean hasPath = lastHost.contains("/");
                String path = hasPath ? lastHost.substring(lastHost.indexOf("/")) : "";
                firstHost += path;

                URI uri = URI.create(firstHost);
                ServiceURI serviceURI = create(uri);

                List<String> multiHosts = new ArrayList<>();
                multiHosts.add(serviceURI.getServiceHosts()[0]);
                multiHosts.addAll(hosts.subList(1, hosts.size()));
                multiHosts = multiHosts
                    .stream()
                    .map(host -> validateHostName(serviceURI.getServiceName(), host))
                    .collect(Collectors.toList());
                return new ServiceURI(
                    serviceURI.getServiceName(),
                    serviceURI.getServiceInfos(),
                    serviceURI.getServiceUser(),
                    multiHosts.toArray(new String[multiHosts.size()]),
                    serviceURI.getServicePath(),
                    serviceURI.getUri());
            }
        }

        // a service uri first should be a valid java.net.URI
        URI uri = URI.create(uriStr);

        return create(uri);
    }

    /**
     * Create a service uri instance from a {@link URI} instance.
     *
     * @param uri {@link URI} instance
     * @return a service uri instance
     * @throws NullPointerException if {@code uriStr} is null
     * @throws IllegalArgumentException if the given string violates RFC&nbsp;2396
     */
    public static ServiceURI create(URI uri) {
        checkNotNull(uri, "service uri instance is null");

        String serviceName;
        String[] serviceInfos = new String[0];
        String scheme = uri.getScheme();
        if (null != scheme) {
            scheme = scheme.toLowerCase();
            final String serviceSep;
            if (scheme.startsWith(SERVICE_DLOG)) {
                serviceSep = SERVICE_DLOG_SEP;
            } else {
                serviceSep = SERVICE_SEP;
            }
            String[] schemeParts = StringUtils.split(scheme, serviceSep);
            serviceName = schemeParts[0];
            serviceInfos = new String[schemeParts.length - 1];
            System.arraycopy(schemeParts, 1, serviceInfos, 0, serviceInfos.length);
        } else {
            serviceName = null;
        }

        String userAndHostInformation = uri.getAuthority();
        checkArgument(!Strings.isNullOrEmpty(userAndHostInformation),
            "authority component is missing in service uri : " + uri);

        String serviceUser;
        List<String> serviceHosts;
        int atIndex = userAndHostInformation.indexOf('@');
        Splitter splitter = Splitter.on(CharMatcher.anyOf(",;"));
        if (atIndex > 0) {
            serviceUser = userAndHostInformation.substring(0, atIndex);
            serviceHosts = splitter.splitToList(userAndHostInformation.substring(atIndex + 1));
        } else {
            serviceUser = null;
            serviceHosts = splitter.splitToList(userAndHostInformation);
        }
        serviceHosts = serviceHosts
            .stream()
            .map(host -> validateHostName(serviceName, host))
            .collect(Collectors.toList());

        String servicePath = uri.getPath();
        checkArgument(null != servicePath,
            "service path component is missing in service uri : " + uri);

        return new ServiceURI(
            serviceName,
            serviceInfos,
            serviceUser,
            serviceHosts.toArray(new String[serviceHosts.size()]),
            servicePath,
            uri);
    }

    private static String validateHostName(String serviceName, String hostname) {
        URI uri = null;
        try {
            uri = URI.create("dummyscheme://" + hostname);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid hostname : " + hostname);
        }
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid hostname : " + hostname);
        }
        int port = uri.getPort();
        if (port == -1 && !serviceName.toLowerCase().equals(SERVICE_BK)) {
            return host;
        } else if (port == -1) {
            port = SERVICE_BK_PORT;
        }
        return host + ":" + port;
    }

    private final String serviceName;
    private final String[] serviceInfos;
    private final String serviceUser;
    private final String[] serviceHosts;
    private final String servicePath;
    private final URI uri;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public String[] getServiceInfos() {
        return serviceInfos;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public String[] getServiceHosts() {
        return serviceHosts;
    }

}
