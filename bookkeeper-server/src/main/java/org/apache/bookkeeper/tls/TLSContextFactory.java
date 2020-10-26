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

import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to manage TLS contexts.
 */
public class TLSContextFactory implements SecurityHandlerFactory {

    static {
        // Fixes loading PKCS8Key file: https://stackoverflow.com/a/18912362
        java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    /**
     * Supported Key File Types.
     */
    public enum KeyStoreType {
        PKCS12("PKCS12"),
        JKS("JKS"),
        PEM("PEM");

        private String str;

        KeyStoreType(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return this.str;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TLSContextFactory.class);
    private static final String TLSCONTEXT_HANDLER_NAME = "tls";
    private String[] protocols;
    private String[] ciphers;
    private volatile SslContext sslContext;
    private ByteBufAllocator allocator;
    private AbstractConfiguration config;
    private FileModifiedTimeUpdater tlsCertificateFilePath, tlsKeyStoreFilePath, tlsKeyStorePasswordFilePath,
            tlsTrustStoreFilePath, tlsTrustStorePasswordFilePath;
    private long certRefreshTime;
    private volatile long certLastRefreshTime;
    private boolean isServerCtx;

    private String getPasswordFromFile(String path) throws IOException {
        byte[] pwd;
        File passwdFile = new File(path);
        if (passwdFile.length() == 0) {
            return "";
        }
        pwd = FileUtils.readFileToByteArray(passwdFile);
        return new String(pwd, StandardCharsets.UTF_8);
    }

    @SuppressFBWarnings(
        value = "OBL_UNSATISFIED_OBLIGATION",
        justification = "work around for java 9: https://github.com/spotbugs/spotbugs/issues/493")
    private KeyStore loadKeyStore(String keyStoreType, String keyStoreLocation, String keyStorePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance(keyStoreType);

        try (FileInputStream ksin = new FileInputStream(keyStoreLocation)) {
            ks.load(ksin, keyStorePassword.trim().toCharArray());
        }
        return ks;
    }

    @Override
    public String getHandlerName() {
        return TLSCONTEXT_HANDLER_NAME;
    }

    private KeyManagerFactory initKeyManagerFactory(String keyStoreType, String keyStoreLocation,
            String keyStorePasswordPath) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException, InvalidKeySpecException {
        KeyManagerFactory kmf = null;

        if (Strings.isNullOrEmpty(keyStoreLocation)) {
            LOG.error("Key store location cannot be empty when Mutual Authentication is enabled!");
            throw new SecurityException("Key store location cannot be empty when Mutual Authentication is enabled!");
        }

        String keyStorePassword = "";
        if (!Strings.isNullOrEmpty(keyStorePasswordPath)) {
            keyStorePassword = getPasswordFromFile(keyStorePasswordPath);
        }

        // Initialize key file
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

        // Initialize trust file
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

    private void createClientContext()
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, NoSuchProviderException {
        ClientConfiguration clientConf = (ClientConfiguration) config;
        markAutoCertRefresh(clientConf.getTLSCertificatePath(), clientConf.getTLSKeyStore(),
                clientConf.getTLSKeyStorePasswordPath(), clientConf.getTLSTrustStore(),
                clientConf.getTLSTrustStorePasswordPath());
        updateClientContext();
    }

    private synchronized void updateClientContext()
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, NoSuchProviderException {
        final SslContextBuilder sslContextBuilder;
        final ClientConfiguration clientConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(config instanceof ClientConfiguration)) {
            throw new SecurityException("Client configruation not provided");
        }

        clientConf = (ClientConfiguration) config;
        provider = getTLSProvider(clientConf.getTLSProvider());
        clientAuthentication = clientConf.getTLSClientAuthentication();

        switch (KeyStoreType.valueOf(clientConf.getTLSTrustStoreType())) {
        case PEM:
            if (Strings.isNullOrEmpty(clientConf.getTLSTrustStore())) {
                throw new SecurityException("CA Certificate required");
            }

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(new File(clientConf.getTLSTrustStore()))
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            TrustManagerFactory tmf = initTrustManagerFactory(clientConf.getTLSTrustStoreType(),
                    clientConf.getTLSTrustStore(), clientConf.getTLSTrustStorePasswordPath());

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(tmf)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        default:
            throw new SecurityException("Invalid Truststore type: " + clientConf.getTLSTrustStoreType());
        }

        if (clientAuthentication) {
            switch (KeyStoreType.valueOf(clientConf.getTLSKeyStoreType())) {
            case PEM:
                final String keyPassword;

                if (Strings.isNullOrEmpty(clientConf.getTLSCertificatePath())) {
                    throw new SecurityException("Valid Certificate is missing");
                }

                if (Strings.isNullOrEmpty(clientConf.getTLSKeyStore())) {
                    throw new SecurityException("Valid Key is missing");
                }

                if (!Strings.isNullOrEmpty(clientConf.getTLSKeyStorePasswordPath())) {
                    keyPassword = getPasswordFromFile(clientConf.getTLSKeyStorePasswordPath());
                } else {
                    keyPassword = null;
                }

                sslContextBuilder.keyManager(new File(clientConf.getTLSCertificatePath()),
                        new File(clientConf.getTLSKeyStore()), keyPassword);
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                KeyManagerFactory kmf = initKeyManagerFactory(clientConf.getTLSKeyStoreType(),
                        clientConf.getTLSKeyStore(), clientConf.getTLSKeyStorePasswordPath());

                sslContextBuilder.keyManager(kmf);
                break;
            default:
                throw new SecurityException("Invalid Keyfile type" + clientConf.getTLSKeyStoreType());
            }
        }

        sslContext = sslContextBuilder.build();
        certLastRefreshTime = System.currentTimeMillis();
    }

    private void createServerContext()
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, IllegalArgumentException {
        isServerCtx = true;
        ServerConfiguration clientConf = (ServerConfiguration) config;
        markAutoCertRefresh(clientConf.getTLSCertificatePath(), clientConf.getTLSKeyStore(),
                clientConf.getTLSKeyStorePasswordPath(), clientConf.getTLSTrustStore(),
                clientConf.getTLSTrustStorePasswordPath());
        updateServerContext();
    }

    private synchronized SslContext getSSLContext() {
        long now = System.currentTimeMillis();
        if ((certRefreshTime > 0 && now > (certLastRefreshTime + certRefreshTime))) {
            if (tlsCertificateFilePath.checkAndRefresh() || tlsKeyStoreFilePath.checkAndRefresh()
                    || tlsKeyStorePasswordFilePath.checkAndRefresh() || tlsTrustStoreFilePath.checkAndRefresh()
                    || tlsTrustStorePasswordFilePath.checkAndRefresh()) {
                try {
                    LOG.info("Updating tls certs certFile={}, keyStoreFile={}, trustStoreFile={}",
                            tlsCertificateFilePath.getFileName(), tlsKeyStoreFilePath.getFileName(),
                            tlsTrustStoreFilePath.getFileName());
                    if (isServerCtx) {
                        updateServerContext();
                    } else {
                        updateClientContext();
                    }
                } catch (Exception e) {
                    LOG.info("Failed to refresh tls certs", e);
                }
            }
        }
        return sslContext;
    }

    private synchronized void updateServerContext() throws SecurityException, KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException,
            InvalidKeySpecException, IllegalArgumentException {
        final SslContextBuilder sslContextBuilder;
        final ServerConfiguration serverConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(config instanceof ServerConfiguration)) {
            throw new SecurityException("Server configruation not provided");
        }

        serverConf = (ServerConfiguration) config;
        provider = getTLSProvider(serverConf.getTLSProvider());
        clientAuthentication = serverConf.getTLSClientAuthentication();

        switch (KeyStoreType.valueOf(serverConf.getTLSKeyStoreType())) {
        case PEM:
            final String keyPassword;

            if (Strings.isNullOrEmpty(serverConf.getTLSKeyStore())) {
                throw new SecurityException("Key path is required");
            }

            if (Strings.isNullOrEmpty(serverConf.getTLSCertificatePath())) {
                throw new SecurityException("Certificate path is required");
            }

            if (!Strings.isNullOrEmpty(serverConf.getTLSKeyStorePasswordPath())) {
                keyPassword = getPasswordFromFile(serverConf.getTLSKeyStorePasswordPath());
            } else {
                keyPassword = null;
            }

            sslContextBuilder = SslContextBuilder
                                .forServer(new File(serverConf.getTLSCertificatePath()),
                            new File(serverConf.getTLSKeyStore()), keyPassword)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            KeyManagerFactory kmf = initKeyManagerFactory(serverConf.getTLSKeyStoreType(),
                    serverConf.getTLSKeyStore(),
                    serverConf.getTLSKeyStorePasswordPath());

            sslContextBuilder = SslContextBuilder.forServer(kmf)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        default:
            throw new SecurityException("Invalid Keyfile type" + serverConf.getTLSKeyStoreType());
        }

        if (clientAuthentication) {
            sslContextBuilder.clientAuth(ClientAuth.REQUIRE);

            switch (KeyStoreType.valueOf(serverConf.getTLSTrustStoreType())) {
            case PEM:
                if (Strings.isNullOrEmpty(serverConf.getTLSTrustStore())) {
                    throw new SecurityException("CA Certificate chain is required");
                }
                sslContextBuilder.trustManager(new File(serverConf.getTLSTrustStore()));
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                TrustManagerFactory tmf = initTrustManagerFactory(serverConf.getTLSTrustStoreType(),
                        serverConf.getTLSTrustStore(), serverConf.getTLSTrustStorePasswordPath());
                sslContextBuilder.trustManager(tmf);
                break;
            default:
                throw new SecurityException("Invalid Truststore type" + serverConf.getTLSTrustStoreType());
            }
        }

        sslContext = sslContextBuilder.build();
        certLastRefreshTime = System.currentTimeMillis();
    }

    @Override
    public synchronized void init(NodeType type, AbstractConfiguration conf, ByteBufAllocator allocator)
            throws SecurityException {
        this.allocator = allocator;
        this.config = conf;
        final String enabledProtocols;
        final String enabledCiphers;
        certRefreshTime = TimeUnit.SECONDS.toMillis(conf.getTLSCertFilesRefreshDurationSeconds());

        enabledCiphers = conf.getTLSEnabledCipherSuites();
        enabledProtocols = conf.getTLSEnabledProtocols();

        try {
            switch (type) {
            case Client:
                createClientContext();
                break;
            case Server:
                createServerContext();
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
            throw new SecurityException("Error initializing SSLContext", e);
        } catch (UnrecoverableKeyException e) {
            throw new SecurityException("Unable to load key manager, possibly bad password", e);
        } catch (InvalidKeySpecException e) {
            throw new SecurityException("Unable to load key manager", e);
        } catch (IllegalArgumentException e) {
            throw new SecurityException("Invalid TLS configuration", e);
        } catch (NoSuchProviderException e) {
            throw new SecurityException("No such provider", e);
        }
    }

    @Override
    public SslHandler newTLSHandler() {
        SslHandler sslHandler = getSSLContext().newHandler(allocator);

        if (protocols != null && protocols.length != 0) {
            sslHandler.engine().setEnabledProtocols(protocols);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enabled cipher protocols: {} ", Arrays.toString(sslHandler.engine().getEnabledProtocols()));
        }

        if (ciphers != null && ciphers.length != 0) {
            sslHandler.engine().setEnabledCipherSuites(ciphers);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enabled cipher suites: {} ", Arrays.toString(sslHandler.engine().getEnabledCipherSuites()));
        }

        return sslHandler;
    }

    private void markAutoCertRefresh(String tlsCertificatePath, String tlsKeyStore, String tlsKeyStorePasswordPath,
            String tlsTrustStore, String tlsTrustStorePasswordPath) {
        tlsCertificateFilePath = new FileModifiedTimeUpdater(tlsCertificatePath);
        tlsKeyStoreFilePath = new FileModifiedTimeUpdater(tlsKeyStore);
        tlsKeyStorePasswordFilePath = new FileModifiedTimeUpdater(tlsKeyStorePasswordPath);
        tlsTrustStoreFilePath = new FileModifiedTimeUpdater(tlsTrustStore);
        tlsTrustStorePasswordFilePath = new FileModifiedTimeUpdater(tlsTrustStorePasswordPath);
    }
}
