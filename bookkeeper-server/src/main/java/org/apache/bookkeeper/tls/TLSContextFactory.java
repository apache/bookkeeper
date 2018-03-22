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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import javax.crypto.NoSuchPaddingException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to manage TLS contexts.
 */
public class TLSContextFactory implements SecurityHandlerFactory {
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
    private AbstractConfiguration conf;
    private NodeType nodeType;

    public String getHandlerName() {
        return TLSCONTEXT_HANDLER_NAME;
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

    private SslContext createClientContext(AbstractConfiguration conf)
            throws SecurityException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException,
            UnrecoverableKeyException, InvalidKeySpecException, InvalidAlgorithmParameterException, KeyException,
            NoSuchPaddingException {
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

        switch (KeyStoreType.valueOf(clientConf.getTLSTrustStoreType())) {
        case PEM:
            if (Strings.isNullOrEmpty(clientConf.getTLSTrustStore())) {
                throw new SecurityException("CA Certificate required");
            }

            X509Certificate[] trustChain = TLSUtils.getCertificates(clientConf.getTLSTrustStore());
            LOG.info("Using Trust Chain: {}", TLSUtils.prettyPrintCertChain(trustChain));

            sslContextBuilder = SslContextBuilder.forClient()
                    .trustManager(trustChain)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .clientAuth(ClientAuth.REQUIRE);


            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            TrustManagerFactory tmf = TLSUtils.initTrustManagerFactory(clientConf.getTLSTrustStoreType(),
                    clientConf.getTLSTrustStore(), clientConf.getTLSTrustStorePasswordPath());
            LOG.info("Using Trust Chain: {}", tmf);

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
                    keyPassword = TLSUtils.getPasswordFromFile(clientConf.getTLSKeyStorePasswordPath());
                } else {
                    keyPassword = null;
                }

                X509Certificate[] certificates = TLSUtils.getCertificates(clientConf.getTLSCertificatePath());
                PrivateKey privateKey = TLSUtils.getPrivateKey(clientConf.getTLSKeyStore(), keyPassword);
                LOG.info("Using Credentials: {}", TLSUtils.prettyPrintCertChain(certificates));

                sslContextBuilder.keyManager(privateKey, certificates);

                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                KeyManagerFactory kmf = TLSUtils.initKeyManagerFactory(clientConf.getTLSKeyStoreType(),
                        clientConf.getTLSKeyStore(), clientConf.getTLSKeyStorePasswordPath());
                LOG.info("Using Credentials: {}", kmf);

                sslContextBuilder.keyManager(kmf);
                break;
            default:
                throw new SecurityException("Invalid Keyfile type: " + clientConf.getTLSKeyStoreType());
            }
        }

        return sslContextBuilder.build();
    }

    private SslContext createServerContext(AbstractConfiguration conf) throws SecurityException, KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException,
            InvalidKeySpecException, InvalidAlgorithmParameterException, KeyException, NoSuchPaddingException {
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
                keyPassword = TLSUtils.getPasswordFromFile(serverConf.getTLSKeyStorePasswordPath());
            } else {
                keyPassword = null;
            }

            X509Certificate[] certificates = TLSUtils.getCertificates(serverConf.getTLSCertificatePath());
            PrivateKey privateKey = TLSUtils.getPrivateKey(serverConf.getTLSKeyStore(), keyPassword);
            LOG.info("Using Credentials: {}", TLSUtils.prettyPrintCertChain(certificates));

            sslContextBuilder = SslContextBuilder.forServer(privateKey, certificates)
                    .ciphers(null)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(provider)
                    .startTls(true);

            break;
        case JKS:
            // falling thru, same as PKCS12
        case PKCS12:
            KeyManagerFactory kmf = TLSUtils.initKeyManagerFactory(serverConf.getTLSKeyStoreType(),
                    serverConf.getTLSKeyStore(),
                    serverConf.getTLSKeyStorePasswordPath());
            LOG.info("Using Credentials: {}", kmf);

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

                X509Certificate[] trustChain = TLSUtils.getCertificates(serverConf.getTLSTrustStore());
                LOG.info("Using Trust chain: {}", TLSUtils.prettyPrintCertChain(trustChain));
                sslContextBuilder.trustManager(trustChain);
                break;
            case JKS:
                // falling thru, same as PKCS12
            case PKCS12:
                TrustManagerFactory tmf = TLSUtils.initTrustManagerFactory(serverConf.getTLSTrustStoreType(),
                        serverConf.getTLSTrustStore(), serverConf.getTLSTrustStorePasswordPath());
                LOG.info("Using Trust chain: {}", tmf);
                sslContextBuilder.trustManager(tmf);
                break;
            default:
                throw new SecurityException("Invalid Truststore type" + serverConf.getTLSTrustStoreType());
            }
        }

        return sslContextBuilder.build();
    }

    @Override
    public synchronized void init(NodeType type, AbstractConfiguration conf) {
        this.conf = conf;
        this.nodeType = type;

        final String enabledProtocols;
        final String enabledCiphers;

        enabledCiphers = conf.getTLSEnabledCipherSuites();
        enabledProtocols = conf.getTLSEnabledProtocols();

        if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
            protocols = enabledProtocols.split(",");
        }

        if (enabledCiphers != null && !enabledCiphers.isEmpty()) {
            ciphers = enabledCiphers.split(",");
        }
    }

    private SslContext createSSLContext()  throws SecurityException {
        try {
            switch (nodeType) {
            case Client:
                return createClientContext(conf);
            case Server:
                return createServerContext(conf);
            default:
                throw new SecurityException(new IllegalArgumentException("Invalid NodeType"));
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
        } catch (KeyException | NoSuchPaddingException | InvalidAlgorithmParameterException e) {
            throw new SecurityException("Invalid Key file", e);
        }
    }

    @Override
    public SslHandler newTLSHandler() {
        SslContext sslContext;
        try {
            sslContext = createSSLContext();
        } catch (SecurityException e) {
            LOG.error("Failed to create SSL Context: ", e);
            return null;
        }

        SslHandler sslHandler = sslContext.newHandler(PooledByteBufAllocator.DEFAULT);

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
}
