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
package org.apache.bookkeeper.tls;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class TLSContextFactory implements SecurityHandlerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(TLSContextFactory.class);
    private final static String TLSCONTEXT_HANDLER_NAME = "tls";
    private String[] protocols;
    private String[] ciphers;
    private SslContext sslContext;

    private String getPasswordFromFile(String path) throws IOException {
        FileInputStream pwdin = new FileInputStream(path);
        byte[] pwd;
        try {
            File passwdFile = new File(path);
            if (passwdFile.length() == 0) {
                return "";
            }
            pwd = FileUtils.readFileToByteArray(passwdFile);
        } finally {
            pwdin.close();
        }
        return new String(pwd, "UTF-8");
    }

    private KeyStore loadKeyStore(String keyStoreType, String keyStoreLocation, String keyStorePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        FileInputStream ksin = new FileInputStream(keyStoreLocation);
        try {
            ks.load(ksin, keyStorePassword.trim().toCharArray());
        } finally {
            ksin.close();
        }
        return ks;
    }

    public String getHandlerName() {
        return TLSCONTEXT_HANDLER_NAME;
    }

    private KeyManagerFactory initKeyManagerFactory(String keyStoreType, String keyStoreLocation,
            String keyStorePasswordPath) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException {
        KeyManagerFactory kmf = null;

        if (Strings.isNullOrEmpty(keyStoreLocation)) {
            LOG.error("Key store location cannot be empty when Mutual Authentication is enabled!");
            throw new SecurityException("Key store location cannot be empty when Mutual Authentication is enabled!");
        }

        String keyStorePassword = "";
        if (!Strings.isNullOrEmpty(keyStorePasswordPath)) {
            keyStorePassword = getPasswordFromFile(keyStorePasswordPath);
        }

        // Initialize key store
        KeyStore ks = loadKeyStore(keyStoreType, keyStoreLocation, keyStorePassword);
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyStorePassword.trim().toCharArray());

        return kmf;
    }

    private TrustManagerFactory initTrustManagerFactory(String trustStoreType, String trustStoreLocation,
            String trustStorePasswordPath)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, SecurityException {
        TrustManagerFactory tmf;

        if (Strings.isNullOrEmpty(trustStoreLocation)) {
            LOG.error("Trust Store location cannot be empty!");
            throw new SecurityException("Trust Store location cannot be empty!");
        }

        String trustStorePassword = "";
        if (!Strings.isNullOrEmpty(trustStorePasswordPath)) {
            trustStorePassword = getPasswordFromFile(trustStorePasswordPath);
        }

        // Initialize trust store
        KeyStore ts = loadKeyStore(trustStoreType, trustStoreLocation, trustStorePassword);
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);

        return tmf;
    }

    private SslProvider getTLSProvider(String sslProvider) {
        if (sslProvider.trim().equalsIgnoreCase("OpenSSL")) {
            if (OpenSsl.isAvailable()) {
                LOG.info("Security provider - OpenSSL");
                return SslProvider.OPENSSL;
            }

            Throwable causeUnavailable = OpenSsl.unavailabilityCause();
            LOG.warn("OpenSSL Unavailable: ", causeUnavailable);

            LOG.info("Security provider - JDK");
            return SslProvider.JDK;
        }

        LOG.info("Security provider - JDK");
        return SslProvider.JDK;
    }

    private void createClientContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException {
        final SslContextBuilder sslContextBuilder;
        final ClientConfiguration clientConf;
        final SslProvider provider;
        final boolean Authentication;

        KeyManagerFactory kmf = null;
        TrustManagerFactory tmf = null;

        // get key-store and trust-store locations and passwords
        if (!(conf instanceof ClientConfiguration)) {
            throw new SecurityException("Client configruation not provided");
        }

        clientConf = (ClientConfiguration) conf;
        provider = getTLSProvider(clientConf.getTLSProvider());
        Authentication = clientConf.getTLSClientAuthentication();

        tmf = initTrustManagerFactory(clientConf.getTLSTrustStoreType(), clientConf.getTLSTrustStore(),
                clientConf.getTLSTrustStorePasswordPath());

        if (Authentication) {
            kmf = initKeyManagerFactory(clientConf.getTLSKeyStoreType(), clientConf.getTLSKeyStore(),
                    clientConf.getTLSKeyStorePasswordPath());
        }

        // Build Ssl context
        sslContextBuilder = SslContextBuilder.forClient()
                                            .trustManager(tmf)
                                            .ciphers(null)
                                            .sessionCacheSize(0)
                                            .sessionTimeout(0)
                                            .sslProvider(provider)
                                            .clientAuth(ClientAuth.REQUIRE);

        /* if mutual authentication is enabled */
        if (Authentication) {
            sslContextBuilder.keyManager(kmf);
        }

        sslContext = sslContextBuilder.build();
    }

    private void createServerContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException {
        final SslContextBuilder sslContextBuilder;
        final ServerConfiguration serverConf;
        final SslProvider provider;
        final boolean Authentication;

        KeyManagerFactory kmf = null;
        TrustManagerFactory tmf = null;

        // get key-store and trust-store locations and passwords
        if (!(conf instanceof ServerConfiguration)) {
            throw new SecurityException("Server configruation not provided");
        }

        serverConf = (ServerConfiguration) conf;
        provider = getTLSProvider(serverConf.getTLSProvider());
        Authentication = serverConf.getTLSClientAuthentication();

        kmf = initKeyManagerFactory(serverConf.getTLSKeyStoreType(), serverConf.getTLSKeyStore(),
                serverConf.getTLSKeyStorePasswordPath());

        if (Authentication) {
            tmf = initTrustManagerFactory(serverConf.getTLSTrustStoreType(), serverConf.getTLSTrustStore(),
                    serverConf.getTLSTrustStorePasswordPath());
        }

        // Build Ssl context
        sslContextBuilder = SslContextBuilder.forServer(kmf)
                                            .ciphers(null)
                                            .sessionCacheSize(0)
                                            .sessionTimeout(0)
                                            .sslProvider(provider)
                                            .startTls(true);

        /* if mutual authentication is enabled */
        if (Authentication) {
            sslContextBuilder.trustManager(tmf)
                            .clientAuth(ClientAuth.REQUIRE);
        }

        sslContext = sslContextBuilder.build();
    }

    @Override
    public synchronized void init(NodeType type, AbstractConfiguration conf) throws SecurityException {
        final String enabledProtocols;
        final String enabledCiphers;

        enabledCiphers = conf.getTLSEnabledCipherSuites();
        enabledProtocols = conf.getTLSEnabledProtocols();

        try {
            switch (type) {
            case Client:
                createClientContext(conf);
                break;
            case Server:
                createServerContext(conf);
                break;
            default:
                throw new SecurityException(new IllegalArgumentException("Invalid NodeType"));
            }

            if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
                protocols = enabledProtocols.split(",");
            }

            if (enabledCiphers != null && !enabledCiphers.isEmpty()) {
                ciphers = enabledCiphers.split(",");
            }
        } catch (KeyStoreException e) {
            throw new RuntimeException("Standard keystore type missing", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Standard algorithm missing", e);
        } catch (CertificateException e) {
            throw new SecurityException("Unable to load keystore", e);
        } catch (IOException e) {
            throw new SecurityException("Error initializing TLSContext", e);
        } catch (UnrecoverableKeyException e) {
            throw new SecurityException("Unable to load key manager, possibly wrong password given", e);
        }
    }

    @Override
    public SslHandler newTLSHandler() {
        SslHandler sslHandler = sslContext.newHandler(PooledByteBufAllocator.DEFAULT);

        if (protocols != null && protocols.length != 0) {
            sslHandler.engine().setEnabledProtocols(protocols);
        }

        if (ciphers != null && ciphers.length != 0) {
            sslHandler.engine().setEnabledCipherSuites(ciphers);
        }

        return sslHandler;
    }
}
