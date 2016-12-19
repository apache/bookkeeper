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
package org.apache.bookkeeper.proto.ssl;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.KeyManagementException;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;

import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLContextFactory {

    static final Logger LOG = LoggerFactory.getLogger(SSLContextFactory.class);

    private final SSLContext ctx;
    private final boolean isClient;
    private String enabledCipherSuites;
    private String enabledProtocols;

    public SSLContextFactory(ServerConfiguration conf) throws SSLConfigurationException {
        isClient = false;
        enabledCipherSuites = conf.getSslEnabledCipherSuites();
        enabledProtocols = conf.getSslEnabledProtocols();
        try {
            KeyStore ks = KeyStore.getInstance("pkcs12");
            String sslKeyStore = conf.getSSLKeyStore();
            InputStream certStream = getClass().getResourceAsStream(sslKeyStore);
            if (certStream == null) {
                LOG.info("Loading Bookie SSLKeyStore from disk path {}", sslKeyStore);
                certStream = new FileInputStream(sslKeyStore);
            } else {
                LOG.info("Found SSLKeyStore from classpath {}", sslKeyStore);
            }
            try {
                ks.load(certStream, conf.getSSLKeyStorePassword().toCharArray());
            } finally {
                certStream.close();
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, conf.getSSLKeyStorePassword().toCharArray());

            // Create the SSL context.
            ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), getTrustManagers(), null);
        } catch (KeyStoreException kse) {
            throw new RuntimeException("Standard keystore type missing", kse);
        } catch (NoSuchAlgorithmException nsae) {
            throw new RuntimeException("Standard algorithm missing", nsae);
        } catch (CertificateException ce) {
            throw new SSLConfigurationException("Unable to load keystore", ce);
        } catch (UnrecoverableKeyException uke) {
            throw new SSLConfigurationException("Unable to load key manager, possibly wrong password given", uke);
        } catch (KeyManagementException | IOException kme) {
            throw new SSLConfigurationException("Error initializing SSLContext", kme);
        }
    }

    public SSLContextFactory(ClientConfiguration conf) throws IOException {
        isClient = true;
        enabledCipherSuites = conf.getSslEnabledCipherSuites();
        enabledProtocols = conf.getSslEnabledProtocols();
        // Create the SSL context.
        try {
            ctx = SSLContext.getInstance("TLS");

            KeyManager[] keyManagers = null;
            if (conf.getClientSSLAuthentication()) {
                KeyStore ks = KeyStore.getInstance("pkcs12");
                String sslKeyStore = conf.getClientSSLKeyStore();
                InputStream certStream = getClass().getResourceAsStream(sslKeyStore);
                if (certStream == null) {
                    LOG.info("Loading BookKeeper client SSLKeyStore from disk path {}", sslKeyStore);
                    certStream = new FileInputStream(sslKeyStore);
                } else {
                    LOG.info("Found BookKeeper client from classpath {}", sslKeyStore);
                }
                try {
                    ks.load(certStream, conf.getClientSSLKeyStorePassword().toCharArray());
                } finally {
                    certStream.close();
                }

                KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                kmf.init(ks, conf.getClientSSLKeyStorePassword().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            if (conf.getVerifySSLCertificates()) {
                ctx.init(keyManagers, null, null);
            } else {
                ctx.init(keyManagers, getTrustManagers(), null);
            }
        } catch (KeyStoreException kse) {
            throw new RuntimeException("Standard keystore type missing", kse);
        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLConfigurationException("Standard algorithm missing", nsae);
        } catch (KeyManagementException | IOException kme) {
            throw new SSLConfigurationException("Error initializing SSLContext", kme);
        } catch (CertificateException ce) {
            throw new SSLConfigurationException("Unable to load keystore", ce);
        } catch (UnrecoverableKeyException uke) {
            throw new SSLConfigurationException("Unable to load key manager, possibly wrong password given", uke);
        }
    }

    public SSLContext getContext() {
        return ctx;
    }

    public SSLEngine getEngine() {
        SSLEngine engine = ctx.createSSLEngine();
        engine.setUseClientMode(isClient);
        if (!isClient) {
            // allow clients to authenticate
            engine.setWantClientAuth(true);
        }
        if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
            String[] protocols = enabledProtocols.split(",");
            engine.setEnabledProtocols(protocols);
        }
        if (enabledCipherSuites != null && !enabledCipherSuites.isEmpty()) {
            String[] ciphers = enabledCipherSuites.split(",");
            engine.setEnabledCipherSuites(ciphers);
        }
        return engine;
    }

    private TrustManager[] getTrustManagers() {
        return new TrustManager[]{new X509TrustManager() {
            // Always trust, even if invalid.

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
                // Always trust.
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
                // Always trust.
            }
        }
        };
    }

}
