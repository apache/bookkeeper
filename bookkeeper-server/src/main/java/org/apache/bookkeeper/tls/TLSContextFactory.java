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
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

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
    /**
     * Supported Key File Types.
     */
    public enum KeyFileType {
        PKCS12, JKS, PEM;
    }

    private static final Logger LOG = LoggerFactory.getLogger(TLSContextFactory.class);
    private static final String TLSCONTEXT_HANDLER_NAME = "tls";
    private String[] protocols;
    private String[] ciphers;
    private SslContext sslContext;

    private String getPasswordFromFile(String path) throws IOException {
        byte[] pwd;
        File passwdFile = new File(path);
        if (passwdFile.length() == 0) {
            return "";
        }
        pwd = FileUtils.readFileToByteArray(passwdFile);
        return new String(pwd, "UTF-8");
    }

    @SuppressFBWarnings(
        value = "OBL_UNSATISFIED_OBLIGATION",
        justification = "work around for java 9: https://github.com/spotbugs/spotbugs/issues/493")
    private KeyStore loadKeyFile(String keyFileType, String keyFileLocation, String keyFilePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance(keyFileType);
        FileInputStream ksin = new FileInputStream(keyFileLocation);
        try {
            ks.load(ksin, keyFilePassword.trim().toCharArray());
        } finally {
            ksin.close();
        }
        return ks;
    }

    public String getHandlerName() {
        return TLSCONTEXT_HANDLER_NAME;
    }

    private KeyManagerFactory initKeyManagerFactory(String keyFileType, String keyFileLocation,
            String keyFilePasswordPath) throws SecurityException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException, UnrecoverableKeyException, InvalidKeySpecException {
        KeyManagerFactory kmf = null;

        if (Strings.isNullOrEmpty(keyFileLocation)) {
            LOG.error("Key store location cannot be empty when Mutual Authentication is enabled!");
            throw new SecurityException("Key store location cannot be empty when Mutual Authentication is enabled!");
        }

        String keyFilePassword = "";
        if (!Strings.isNullOrEmpty(keyFilePasswordPath)) {
            keyFilePassword = getPasswordFromFile(keyFilePasswordPath);
        }

        // Initialize key file
        KeyStore ks = loadKeyFile(keyFileType, keyFileLocation, keyFilePassword);
        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyFilePassword.trim().toCharArray());

        return kmf;
    }

    private TrustManagerFactory initTrustManagerFactory(String trustFileType, String trustFileLocation,
            String trustFilePasswordPath)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, SecurityException {
        TrustManagerFactory tmf;

        if (Strings.isNullOrEmpty(trustFileLocation)) {
            LOG.error("Trust Store location cannot be empty!");
            throw new SecurityException("Trust Store location cannot be empty!");
        }

        String trustFilePassword = "";
        if (!Strings.isNullOrEmpty(trustFilePasswordPath)) {
            trustFilePassword = getPasswordFromFile(trustFilePasswordPath);
        }

        // Initialize trust file
        KeyStore ts = loadKeyFile(trustFileType, trustFileLocation, trustFilePassword);
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

    private void createClientContext(AbstractConfiguration conf)
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, NoSuchProviderException {
        final SslContextBuilder sslContextBuilder;
        final ClientConfiguration clientConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(conf instanceof ClientConfiguration)) {
            throw new SecurityException("Client configruation not provided");
        }

        clientConf = (ClientConfiguration) conf;
        provider = getTLSProvider(clientConf.getTLSProvider());
        clientAuthentication = clientConf.getTLSClientAuthentication();

        switch (KeyFileType.valueOf(clientConf.getTLSTrustFileType())) {
        case PEM:
            if (Strings.isNullOrEmpty(clientConf.getTLSTrustFilePath())) {
                throw new SecurityException("CA Certificate required");
            }

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(new File(clientConf.getTLSTrustFilePath()))
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            TrustManagerFactory tmf = initTrustManagerFactory(clientConf.getTLSTrustFileType(),
                    clientConf.getTLSTrustFilePath(), clientConf.getTLSTrustFilePasswordPath());

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(tmf)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);

            break;
        default:
            throw new SecurityException("Invalid Trustfile type: " + clientConf.getTLSTrustFileType());
        }

        if (clientAuthentication) {
            switch (KeyFileType.valueOf(clientConf.getTLSKeyFileType())) {
            case PEM:
                final String keyPassword;

                if (Strings.isNullOrEmpty(clientConf.getTLSCertificatePath())) {
                    throw new SecurityException("Valid Certificate is missing");
                }

                if (Strings.isNullOrEmpty(clientConf.getTLSKeyFilePath())) {
                    throw new SecurityException("Valid Key is missing");
                }

                if (!Strings.isNullOrEmpty(clientConf.getTLSKeyFilePasswordPath())) {
                    keyPassword = getPasswordFromFile(clientConf.getTLSKeyFilePasswordPath());
                } else {
                    keyPassword = null;
                }

                sslContextBuilder.keyManager(new File(clientConf.getTLSCertificatePath()),
                        new File(clientConf.getTLSKeyFilePath()), keyPassword);
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                KeyManagerFactory kmf = initKeyManagerFactory(clientConf.getTLSKeyFileType(),
                        clientConf.getTLSKeyFilePath(), clientConf.getTLSKeyFilePasswordPath());

                sslContextBuilder.keyManager(kmf);
                break;
            default:
                throw new SecurityException("Invalid Keyfile type" + clientConf.getTLSKeyFileType());
            }
        }

        sslContext = sslContextBuilder.build();
    }

    private void createServerContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException,
            InvalidKeySpecException, NoSuchProviderException {
        final SslContextBuilder sslContextBuilder;
        final ServerConfiguration serverConf;
        final SslProvider provider;
        final boolean clientAuthentication;

        // get key-file and trust-file locations and passwords
        if (!(conf instanceof ServerConfiguration)) {
            throw new SecurityException("Server configruation not provided");
        }

        serverConf = (ServerConfiguration) conf;
        provider = getTLSProvider(serverConf.getTLSProvider());
        clientAuthentication = serverConf.getTLSClientAuthentication();

        switch (KeyFileType.valueOf(serverConf.getTLSKeyFileType())) {
        case PEM:
            final String keyPassword;

            if (Strings.isNullOrEmpty(serverConf.getTLSKeyFilePath())) {
                throw new SecurityException("Key path is required");
            }

            if (Strings.isNullOrEmpty(serverConf.getTLSCertificatePath())) {
                throw new SecurityException("Certificate path is required");
            }

            if (!Strings.isNullOrEmpty(serverConf.getTLSKeyFilePasswordPath())) {
                keyPassword = getPasswordFromFile(serverConf.getTLSKeyFilePasswordPath());
            } else {
                keyPassword = null;
            }

            sslContextBuilder = SslContextBuilder
                                .forServer(new File(serverConf.getTLSCertificatePath()),
                            new File(serverConf.getTLSKeyFilePath()), keyPassword)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            KeyManagerFactory kmf = initKeyManagerFactory(serverConf.getTLSKeyFileType(),
                    serverConf.getTLSKeyFilePath(),
                    serverConf.getTLSKeyFilePasswordPath());

            sslContextBuilder = SslContextBuilder.forServer(kmf)
                                .ciphers(null)
                                .sessionCacheSize(0)
                                .sessionTimeout(0)
                                .sslProvider(provider)
                                .startTls(true);

            break;
        default:
            throw new SecurityException("Invalid Keyfile type" + serverConf.getTLSKeyFileType());
        }

        if (clientAuthentication) {
            sslContextBuilder.clientAuth(ClientAuth.REQUIRE);

            switch (KeyFileType.valueOf(serverConf.getTLSTrustFileType())) {
            case PEM:
                if (Strings.isNullOrEmpty(serverConf.getTLSTrustFilePath())) {
                    throw new SecurityException("CA Certificate chain is required");
                }
                sslContextBuilder.trustManager(new File(serverConf.getTLSTrustFilePath()));
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                TrustManagerFactory tmf = initTrustManagerFactory(serverConf.getTLSTrustFileType(),
                        serverConf.getTLSTrustFilePath(), serverConf.getTLSTrustFilePasswordPath());
                sslContextBuilder.trustManager(tmf);
                break;
            default:
                throw new SecurityException("Invalid Trustfile type" + serverConf.getTLSTrustFileType());
            }
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
            throw new RuntimeException("Standard keyfile type missing", e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Standard algorithm missing", e);
        } catch (CertificateException e) {
            throw new SecurityException("Unable to load keyfile", e);
        } catch (IOException e) {
            throw new SecurityException("Error initializing SSLContext", e);
        } catch (UnrecoverableKeyException e) {
            throw new SecurityException("Unable to load key manager, possibly bad password", e);
        } catch (InvalidKeySpecException e) {
            throw new SecurityException("Unable to load key manager", e);
        } catch (NoSuchProviderException e) {
            throw new SecurityException("No such provider", e);
        }
    }

    @Override
    public SslHandler newTLSHandler() {
        SslHandler sslHandler = sslContext.newHandler(PooledByteBufAllocator.DEFAULT);

        if (protocols != null && protocols.length != 0) {
            sslHandler.engine().setEnabledProtocols(protocols);
        }
        LOG.info("Enabled cipher protocols: {} ", Arrays.toString(sslHandler.engine().getEnabledProtocols()));

        if (ciphers != null && ciphers.length != 0) {
            sslHandler.engine().setEnabledCipherSuites(ciphers);
        }
        LOG.info("Enabled cipher suites: {} ", Arrays.toString(sslHandler.engine().getEnabledCipherSuites()));

        return sslHandler;
    }
}
