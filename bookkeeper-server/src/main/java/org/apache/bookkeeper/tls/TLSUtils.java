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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commonly used TLS util methods.
 */
public class TLSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TLSUtils.class);
    private static final Pattern KEY_PATTERN = Pattern.compile(
            "-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+([a-z0-9+/=\\r\\n]+)-+END\\s+.*PRIVATE\\s+KEY[^-]*-+",
            2);

    public static X509Certificate[] getCertificates(String certFilePath) throws CertificateException, IOException {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        FileInputStream fileInputStream = new FileInputStream(certFilePath);
        Collection<? extends Certificate> certificates = certificateFactory.generateCertificates(fileInputStream);
        fileInputStream.close();

        /* cast the return to X509Certificate */
        return certificates.toArray(new X509Certificate[0]);
    }

    public static PrivateKey getPrivateKey(String keyStorePath, String keyPassword) throws IOException,
            NoSuchAlgorithmException, InvalidKeySpecException, KeyException, InvalidAlgorithmParameterException,
            NoSuchPaddingException {

        File keyStore = new File(keyStorePath);
        FileInputStream fileInputStream = new FileInputStream(keyStore);
        DataInputStream dataInputStream = new DataInputStream(fileInputStream);

        byte[] keyBytes = new byte[(int) keyStore.length()];
        dataInputStream.readFully(keyBytes);
        dataInputStream.close();

        Matcher m = KEY_PATTERN.matcher(new String(keyBytes, StandardCharsets.UTF_8));
        if (!m.find()) {
            throw new KeyException("Could not find PKCS #8 private key in: " + keyStorePath);
        }

        byte[] der = Base64.decodeBase64(m.group(1));
        PKCS8EncodedKeySpec spec = getKeySpec(der, keyPassword);

        try {
            return KeyFactory.getInstance("RSA").generatePrivate(spec);
        } catch (InvalidKeySpecException ikse) {
            try {
                return KeyFactory.getInstance("DSA").generatePrivate(spec);
            } catch (InvalidKeySpecException ikse1) {
                try {
                    return KeyFactory.getInstance("EC").generatePrivate(spec);
                } catch (InvalidKeySpecException ikse2) {
                    throw new InvalidKeySpecException("Neither RSA, DSA nor EC worked", ikse2);
                }
            }
        }
    }

    private static PKCS8EncodedKeySpec getKeySpec(byte[] key, String password) throws IOException,
            NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, InvalidKeyException {
        if (password == null) {
            return new PKCS8EncodedKeySpec(key);
        } else {
            EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(key);
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(encryptedPrivateKeyInfo.getAlgName());
            PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
            SecretKey secretKey = secretKeyFactory.generateSecret(pbeKeySpec);
            Cipher cipher = Cipher.getInstance(encryptedPrivateKeyInfo.getAlgName());
            cipher.init(2, secretKey, encryptedPrivateKeyInfo.getAlgParameters());
            return encryptedPrivateKeyInfo.getKeySpec(cipher);
        }
    }

    public static KeyManagerFactory initKeyManagerFactory(String keyStoreType, String keyStoreLocation,
                                                          String keyStorePasswordPath) throws SecurityException,
            KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException {
        KeyManagerFactory kmf;

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

    public static TrustManagerFactory initTrustManagerFactory(String trustStoreType, String trustStoreLocation,
                                                        String trustStorePasswordPath) throws KeyStoreException,
            NoSuchAlgorithmException, CertificateException, IOException, SecurityException {
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

    public static String getPasswordFromFile(String path) throws IOException {
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
    private static KeyStore loadKeyStore(String keyStoreType, String keyStoreLocation, String keyStorePassword)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        try (FileInputStream ksin = new FileInputStream(keyStoreLocation)) {
            ks.load(ksin, keyStorePassword.trim().toCharArray());
        }
        return ks;
    }

    public static String prettyPrintCertChain(Certificate[] certificates) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(certificates)
                .forEach(cert -> {
                    if (cert instanceof X509Certificate) {
                        X509Certificate c = (X509Certificate) cert;
                        sb.append("[").append(System.lineSeparator())
                            .append("Subject: ").append(c.getSubjectX500Principal().toString())
                                .append(System.lineSeparator())
                            .append("Signature Algorithm: ").append(c.getSigAlgName()).append(System.lineSeparator())
                            .append("Validity: [ From: ").append(c.getNotBefore())
                                .append(", To: ").append(c.getNotAfter()).append("]").append(System.lineSeparator())
                            .append("Issuer: ").append(c.getIssuerDN().toString()).append(System.lineSeparator())
                            .append("]");
                    } else {
                        sb.append(cert.toString());
                    }
                });
        return sb.toString();
    }
}
