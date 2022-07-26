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

package org.apache.bookkeeper.tests.containers.wait;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.rnorth.ducttape.unreliables.Unreliables.retryUntilSuccess;

import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.rnorth.ducttape.TimeoutException;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

/**
 * An wait strategy to wait for http ports.
 */
public class HttpWaitStrategy extends AbstractWaitStrategy {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HttpWaitStrategy.class);
    /**
     * Authorization HTTP header.
     */
    private static final String HEADER_AUTHORIZATION = "Authorization";
    /**
     * Basic Authorization scheme prefix.
     */
    private static final String AUTH_BASIC = "Basic ";
    private String path = "/";
    private int statusCode = HttpURLConnection.HTTP_OK;
    private boolean tlsEnabled;
    private String username;
    private String password;
    private Predicate<String> responsePredicate;
    private int port = 80;

    /**
     * Waits for the given status code.
     *
     * @param statusCode the expected status code
     * @return this
     */
    public HttpWaitStrategy forStatusCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    /**
     * Waits for the given path.
     *
     * @param path the path to check
     * @return this
     */
    public HttpWaitStrategy forPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * Wait for the given port.
     *
     * @param port the given port
     * @return this
     */
    public HttpWaitStrategy forPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Indicates that the status check should use HTTPS.
     *
     * @return this
     */
    public HttpWaitStrategy usingTls() {
        this.tlsEnabled = true;
        return this;
    }

    /**
     * Authenticate with HTTP Basic Authorization credentials.
     *
     * @param username the username
     * @param password the password
     * @return this
     */
    public HttpWaitStrategy withBasicCredentials(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Waits for the response to pass the given predicate.
     *
     * @param responsePredicate The predicate to test the response against
     * @return this
     */
    public HttpWaitStrategy forResponsePredicate(Predicate<String> responsePredicate) {
        this.responsePredicate = responsePredicate;
        return this;
    }

    @Override
    protected void waitUntilReady() {
        final String containerName = waitStrategyTarget.getContainerInfo().getName();
        final int livenessCheckPort = waitStrategyTarget.getMappedPort(port);
        final String uri = buildLivenessUri(livenessCheckPort).toString();
        log.info("{}: Waiting for {} seconds for URL: {}", containerName, startupTimeout.getSeconds(), uri);
        // try to connect to the URL
        try {
            retryUntilSuccess((int) startupTimeout.getSeconds(), TimeUnit.SECONDS, () -> {
                getRateLimiter().doWhenReady(() -> {
                    try {
                        final HttpURLConnection connection = (HttpURLConnection) new URL(uri).openConnection();
                        // authenticate
                        if (!Strings.isNullOrEmpty(username)) {
                            connection.setRequestProperty(HEADER_AUTHORIZATION, buildAuthString(username, password));
                            connection.setUseCaches(false);
                        }
                        connection.setRequestMethod("GET");
                        connection.connect();
                        if (statusCode != connection.getResponseCode()) {
                            throw new RuntimeException(String.format("HTTP response code was: %s",
                                connection.getResponseCode()));
                        }
                        if (responsePredicate != null) {
                            String responseBody = getResponseBody(connection);
                            if (!responsePredicate.test(responseBody)) {
                                throw new RuntimeException(String.format("Response: %s did not match predicate",
                                    responseBody));
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                return true;
            });
        } catch (TimeoutException e) {
            throw new ContainerLaunchException(String.format(
                "Timed out waiting for URL to be accessible (%s should return HTTP %s)", uri, statusCode));
        }
    }

    /**
     * Build the URI on which to check if the container is ready.
     *
     * @param livenessCheckPort the liveness port
     * @return the liveness URI
     */
    private URI buildLivenessUri(int livenessCheckPort) {
        final String scheme = (tlsEnabled ? "https" : "http") + "://";
        final String host = waitStrategyTarget.getContainerIpAddress();
        final String portSuffix;
        if ((tlsEnabled && 443 == livenessCheckPort) || (!tlsEnabled && 80 == livenessCheckPort)) {
            portSuffix = "";
        } else {
            portSuffix = ":" + livenessCheckPort;
        }
        return URI.create(scheme + host + portSuffix + path);
    }

    /**
     * @param username the username
     * @param password the password
     * @return a basic authentication string for the given credentials
     */
    private String buildAuthString(String username, String password) {
        return AUTH_BASIC + BaseEncoding.base64().encode((username + ":" + password).getBytes(UTF_8));
    }

    private String getResponseBody(HttpURLConnection connection) throws IOException {
        BufferedReader reader = null;
        try {
            if (200 <= connection.getResponseCode() && connection.getResponseCode() <= 299) {
                reader = new BufferedReader(new InputStreamReader((connection.getInputStream()), UTF_8));
            } else {
                reader = new BufferedReader(new InputStreamReader((connection.getErrorStream()), UTF_8));
            }
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        } finally {
            if (null != reader) {
                reader.close();
            }
        }
    }
}
