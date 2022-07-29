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
package org.apache.bookkeeper.http.vertx;

import java.io.FileInputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.bookkeeper.http.HttpServerConfiguration;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.apache.bookkeeper.http.NullHttpServiceProvider;
import org.junit.Test;

/**
 * Unit test {@link VertxHttpServer}.
 */
public class TestVertxHttpsServer {

    private static final String CLIENT_KEYSTORE_PATH = "./src/test/resources/vertx_client_key.jks";

    private static final String CLIENT_TRUSTSTORE_PATH = "./src/test/resources/vertx_client_trust.jks";

    private static final String CLIENT_WRONG_TRUSTSTORE_PATH = "./src/test/resources/vertx_client_wrong_trust.jks";

    private static final String CLIENT_KEYSTORE_PASSWORD = "vertx_client_pwd";

    private static final String CLIENT_TRUSTSTORE_PASSWORD = "vertx_client_pwd";

    private static final String SERVER_KEYSTORE_PATH = "./src/test/resources/vertx_server_key.jks";

    private static final String SERVER_TRUSTSTORE_PATH = "./src/test/resources/vertx_server_trust.jks";

    private static final String SERVER_KEYSTORE_PASSWORD = "vertx_server_pwd";

    private static final String SERVER_TRUSTSTORE_PASSWORD = "vertx_server_pwd";

    @Test(timeout = 60_000)
    public void testVertxServerTls() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServiceProvider httpServiceProvider = NullHttpServiceProvider.getInstance();
        httpServer.initialize(httpServiceProvider);
        HttpServerConfiguration httpServerConfiguration = new HttpServerConfiguration();
        httpServerConfiguration.setTlsEnable(true);
        httpServerConfiguration.setKeyStorePath(SERVER_KEYSTORE_PATH);
        httpServerConfiguration.setKeyStorePassword(SERVER_KEYSTORE_PASSWORD);
        httpServerConfiguration.setTrustStorePath(SERVER_TRUSTSTORE_PATH);
        httpServerConfiguration.setTrustStorePassword(SERVER_TRUSTSTORE_PASSWORD);
        httpServer.startServer(0, "localhost", httpServerConfiguration);
        int actualPort = httpServer.getListeningPort();
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        // key store
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream inputStream = new FileInputStream(CLIENT_KEYSTORE_PATH)) {
            keyStore.load(inputStream, CLIENT_KEYSTORE_PASSWORD.toCharArray());
        }
        keyManagerFactory.init(keyStore, "vertx_client_pwd".toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        // trust store
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (FileInputStream inputStream = new FileInputStream(CLIENT_TRUSTSTORE_PATH)) {
            trustStore.load(inputStream, CLIENT_TRUSTSTORE_PASSWORD.toCharArray());
        }
        trustManagerFactory.init(trustStore);
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), new SecureRandom());
        URL url = new URL("https://localhost:" + actualPort);
        HttpsURLConnection urlConnection = (HttpsURLConnection) url.openConnection();
        urlConnection.setHostnameVerifier((s, sslSession) -> true);
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        urlConnection.setSSLSocketFactory(socketFactory);
        urlConnection.setRequestMethod("GET");
        urlConnection.getResponseCode();
        httpServer.stopServer();
    }

    @Test(timeout = 60_000, expected = SSLHandshakeException.class)
    public void testVertxServerTlsFailByCertNotMatch() throws Exception {
        VertxHttpServer httpServer = new VertxHttpServer();
        HttpServerConfiguration httpServerConfiguration = new HttpServerConfiguration();
        httpServerConfiguration.setTlsEnable(true);
        httpServerConfiguration.setKeyStorePath(SERVER_KEYSTORE_PATH);
        httpServerConfiguration.setKeyStorePassword(SERVER_KEYSTORE_PASSWORD);
        httpServerConfiguration.setTrustStorePath(SERVER_TRUSTSTORE_PATH);
        httpServerConfiguration.setTrustStorePassword(SERVER_TRUSTSTORE_PASSWORD);
        httpServer.startServer(0, "localhost", httpServerConfiguration);
        int actualPort = httpServer.getListeningPort();
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        // key store
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream inputStream = new FileInputStream(CLIENT_KEYSTORE_PATH)) {
            keyStore.load(inputStream, CLIENT_KEYSTORE_PASSWORD.toCharArray());
        }
        keyManagerFactory.init(keyStore, "vertx_client_pwd".toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        // trust store
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (FileInputStream inputStream = new FileInputStream(CLIENT_WRONG_TRUSTSTORE_PATH)) {
            trustStore.load(inputStream, CLIENT_TRUSTSTORE_PASSWORD.toCharArray());
        }
        trustManagerFactory.init(trustStore);
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), new SecureRandom());
        URL url = new URL("https://localhost:" + actualPort);
        HttpsURLConnection urlConnection = (HttpsURLConnection) url.openConnection();
        urlConnection.setHostnameVerifier((s, sslSession) -> true);
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        urlConnection.setSSLSocketFactory(socketFactory);
        urlConnection.setRequestMethod("GET");
        urlConnection.getResponseCode();
    }

}
