package org.apache.bookkeeper.tls;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateExtensions;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.DNSName;
import sun.security.x509.GeneralName;
import sun.security.x509.GeneralNames;
import sun.security.x509.IPAddressName;
import sun.security.x509.SubjectAlternativeNameExtension;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

/**
 * TLS mocks.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TLSUtils.class)
@PowerMockIgnore({"javax.management.*",
                    "javax.net.ssl.*",
                    "javax.security.*",
                    "sun.security.*",
                    "io.netty.*",
                    "org.apache.zookeeper.*"
                })
public class TestTLSMock extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestTLSMock.class);
    private static final String ALGOSHA1WITHRSA = "sha1WithRSA";

    // root and intermediate CA credentials
    private X509Certificate rootCACert;
    private X509Certificate intermediateCACert;
    private PrivateKey intermediateCAPrivateKey; // required to sign certificates


    public TestTLSMock() throws Exception{
        super(1);

        KeyPair keyPairCA = generateKeyPair();

        //  CA certificate
        Date caStartDate = new Date();
        caStartDate = new Date(caStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        // end date is 10 yrs away
        Date caEndDate = new Date(caStartDate.getTime() + TimeUnit.DAYS.toMillis(3650));
        final String dnCA = "CN=rootCA.apache.bookkeeper.org";
        rootCACert = createSignedCertificate(keyPairCA.getPrivate(), keyPairCA.getPublic(),
                caStartDate, caEndDate, null, dnCA, dnCA, ALGOSHA1WITHRSA);

        try {
            if (!TLSUtils.verifyRSAKeyAndCertificateMatch(rootCACert, keyPairCA.getPrivate())) {
                LOG.error("Root CA Key and Certificate do not match");
            }
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            throw new java.lang.SecurityException("Root CA: Failed to match Key with Certificate", e);
        }

        // intermediate CA certificate
        KeyPair keyPairIntermediateCA = generateKeyPair();
        Date intermediatecaStartDate = new Date();
        /* start time is 1hr old */
        intermediatecaStartDate = new Date(intermediatecaStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        /* start time is 1hr old */
        Date intermediatecaEndDate = new Date(intermediatecaStartDate.getTime() + TimeUnit.DAYS.toMillis(365));
        final String dnIntermediateCA = "CN=intermediateCA.apache.bookkeeper.org";
        intermediateCACert = createSignedCertificate(keyPairCA.getPrivate(), keyPairIntermediateCA.getPublic(),
                intermediatecaStartDate, intermediatecaEndDate, null, dnIntermediateCA, dnCA, ALGOSHA1WITHRSA);
        intermediateCAPrivateKey = keyPairIntermediateCA.getPrivate();

        try {
            if (!TLSUtils.verifyRSAKeyAndCertificateMatch(intermediateCACert, keyPairIntermediateCA.getPrivate())) {
                LOG.error("Intermediate CA Key and Certificate do not match");
            }
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            throw new java.lang.SecurityException("Intermediate CA: Failed to match Key with Certificate", e);
        }

    }

    private String getResourcePath(String resource) throws Exception {
        return this.getClass().getClassLoader().getResource(resource).toURI().getPath();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        /* client configuration */
        baseClientConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseClientConf.setTLSClientAuthentication(true);
        baseClientConf.setTLSKeyStoreType("PEM");
        baseClientConf.setTLSKeyStore(getResourcePath("client-key.pem"));
        baseClientConf.setTLSCertificatePath(
                getResourcePath("client-cert.pem"));
        baseClientConf.setTLSTrustStoreType("PEM");
        baseClientConf.setTLSTrustStore(
                getResourcePath("server-cert.pem"));

        /* server configuration */
        baseConf.setTLSProviderFactoryClass(TLSContextFactory.class.getName());
        baseConf.setTLSClientAuthentication(true);
        baseConf.setTLSKeyStoreType("PEM");
        baseConf.setTLSKeyStore(getResourcePath("server-key.pem"));
        baseConf.setTLSCertificatePath(getResourcePath("server-cert.pem"));
        baseConf.setTLSTrustStoreType("PEM");
        baseConf.setTLSTrustStore(getResourcePath("client-cert.pem"));

        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception{
        super.tearDown();
    }

    private KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        return KeyPairGenerator.getInstance("RSA").generateKeyPair();
    }

    @SuppressWarnings("sunapi")
    private X509Certificate createSignedCertificate(PrivateKey caPrivateKey, PublicKey publicKey,
                                                        Date startDate, Date endDate, String hostId,
                                                        String dn, String dnIssuer, String algo)
            throws IOException, CertificateException, NoSuchProviderException, NoSuchAlgorithmException,
            InvalidKeyException, SignatureException {
        X509CertInfo x509CertInfo = new X509CertInfo();
        CertificateValidity certificateValidity = new CertificateValidity(startDate, endDate);
        BigInteger serial = new BigInteger(64, new SecureRandom());
        X500Name owner = new X500Name(dn);
        X500Name issuer = new X500Name(dnIssuer);

        x509CertInfo.set(X509CertInfo.KEY, new CertificateX509Key(publicKey));
        x509CertInfo.set(X509CertInfo.VALIDITY, certificateValidity);
        x509CertInfo.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(serial));
        x509CertInfo.set(X509CertInfo.SUBJECT, owner);
        x509CertInfo.set(X509CertInfo.ISSUER, issuer);
        x509CertInfo.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
        AlgorithmId algorithmId = AlgorithmId.get(algo);
        x509CertInfo.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algorithmId));

        // set certificate Subject Alternative Name extentions (SAN)
        GeneralNames generalNames = new GeneralNames();
        /* testcases are configured to use loopback interface, so set the hostname and ip accordingly */
        String hostIp = "127.0.0.1";
        String hostName = "localhost";
        if (hostId == null) {
            /* get loopback interface and its ipaddress */
            try {
                Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface nif : Collections.list(nifs)) {
                    if (nif.isLoopback()) {
                        Enumeration<InetAddress> addrs = nif.getInetAddresses();
                        while (addrs.hasMoreElements()) {
                            InetAddress addr = addrs.nextElement();
                            if (addr instanceof Inet4Address && addr.isLoopbackAddress()) {
                                hostIp = addr.getHostAddress();
                                hostName = addr.getHostName();
                                break;
                            }
                        }
                    }
                }
            } catch (SocketException se) {
                LOG.warn("Exception while figuring out loopback interface. Will use default: {}/{}",
                        hostIp, hostName, se);
            }
            generalNames.add(new GeneralName(new IPAddressName(hostIp)));
            generalNames.add(new GeneralName(new DNSName(hostName)));
        } else {
            if (IPAddressUtil.isIPv4LiteralAddress(hostId)
                    || IPAddressUtil.isIPv6LiteralAddress(hostId)) {
                generalNames.add(new GeneralName(new IPAddressName(hostId)));
            } else {
                generalNames.add(new GeneralName(new DNSName(hostId)));
            }
        }

        CertificateExtensions extensions = new CertificateExtensions();
        extensions.set(SubjectAlternativeNameExtension.NAME, new SubjectAlternativeNameExtension(generalNames));
        x509CertInfo.set(X509CertInfo.EXTENSIONS, extensions);

        // Sign the cert
        X509CertImpl cert = new X509CertImpl(x509CertInfo);
        cert.sign(caPrivateKey, algo);

        return cert;
    }

    private void testClient(ClientConfiguration conf, int clusterSize) throws Exception {
        try (BookKeeper client = new BookKeeper(conf)) {
            byte[] passwd = "testPassword".getBytes();
            int numEntries = 10;
            long lid;
            byte[] testEntry = "testEntry".getBytes();
            try (LedgerHandle lh = client.createLedger(clusterSize, clusterSize, DigestType.CRC32, passwd)) {
                for (int i = 0; i <= numEntries; i++) {
                    lh.addEntry(testEntry);
                }
                lid = lh.getId();
            }
            try (LedgerHandle lh = client.openLedger(lid, DigestType.CRC32, passwd)) {
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries);
                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    assertTrue("Entry contents incorrect", Arrays.equals(e.getEntry(), testEntry));
                }
            }
        }
    }


    /*
     * Both server and client uses self signed certificates. Ledger writes should succeed.
     */
    @Test
    public void testValidSelfSignedCertificate() throws Exception {
        Date serverCertStartDate = new Date();
        // end time is now + 1hr
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(serverKeyPair.getPrivate(),
                serverKeyPair.getPublic(), serverCertStartDate, serverCertEndDate, null, dn, dn, ALGOSHA1WITHRSA);

        Date clientCertStartDate = new Date();
        // end time is now + 1hr
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(serverKeyPair.getPrivate(),
                clientKeyPair.getPublic(), clientCertStartDate, clientCertEndDate, null, dn, dn, ALGOSHA1WITHRSA);

        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
            .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
            .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {clientCertificate});

        restartBookies();

        /* set client to trust server certificate */
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {serverCertificate});

        testClient(clientConf, numBookies);
    }

    /*
     * Both server and client uses self signed certificates. Mutual Authentication
     * and TLS handshake succeed even if the client/server or both self signed
     * certificates have expired.
     */
    @Test
    public void testExpiredValiditySelfSignedCertificate() throws Exception {
        Date serverCertStartDate = new Date();
        serverCertStartDate = new Date(serverCertStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        // end time same as start time (expired)
        Date serverCertEndDate = serverCertStartDate;
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(serverKeyPair.getPrivate(),
                serverKeyPair.getPublic(), serverCertStartDate, serverCertEndDate, null, dn, dn, ALGOSHA1WITHRSA);

        Date clientCertStartDate = new Date();
        clientCertStartDate = new Date(clientCertStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        // end time is same as start time (expired)
        Date clientCertEndDate = clientCertStartDate;
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(serverKeyPair.getPrivate(),
                clientKeyPair.getPublic(), clientCertStartDate, clientCertEndDate, null, dn, dn, ALGOSHA1WITHRSA);

        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {clientCertificate});

        restartBookies();

        /* set client to trust server certificate */
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {serverCertificate});

        testClient(clientConf, numBookies);
    }

    /*
     * Certificates are generally signed by a rootCA or an intermediate CA.
     */
    @Test
    public void testValidCAIssuedCertificate() throws Exception {
        Date serverCertStartDate = new Date();
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        testClient(clientConf, numBookies);
    }

    /*
     * Certificates are generally signed by a rootCA or an intermediate CA.
     * A certificate with expired validity will fail TLS handshake.
     *
     * Creating server cert with expired validity will verify both
     * certificate validity and client side MutualTLS.
     */
    @Test
    public void testExpiredCAIssuedServerCertificate() throws Exception {
        Date serverCertStartDate = new Date();
        /* start time is 1hr old */
        serverCertStartDate = new Date(serverCertStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        Date serverCertEndDate = serverCertStartDate; /* end time is same as start time (expired cert) */
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        try {
            testClient(clientConf, numBookies);
            fail("ledger Write should have failed with expired certificate");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // valid
        }
    }

    /*
     * Verify if certificate expires while the connection is active,
     * in progress communication is not affected.
     */
    @Test
    public void testCertificateExpiryInLongLivedSSLSession() throws Exception {
        Date serverCertStartDate = new Date();
        // end time is now + 10s
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.SECONDS.toMillis(10));
        /* start time is 1hr old */
        serverCertStartDate = new Date(serverCertStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        try (BookKeeper client = new BookKeeper(clientConf)) {
            byte[] passwd = "testPassword".getBytes();
            int numEntries = 10;
            byte[] testEntry = "testEntry".getBytes();
            LedgerHandle lh = client.createLedger(numBookies, numBookies, DigestType.CRC32, passwd);
            try {
                for (int i = 0; i <= numEntries; i++) {
                    lh.addEntry(testEntry);
                }
            } catch (BKException.BKNotEnoughBookiesException e) {
                fail("Ledger writes should not have failed");
            }

            long now = System.currentTimeMillis(); // same as Date#getTime()
            // sleep till certificate expires
            if (now < serverCertEndDate.getTime()) {
                Thread.sleep(serverCertEndDate.getTime() - now);
            }

            // adding more entries after certificate expiry should succeed
            for (int i = 0; i <= numEntries; i++) {
                lh.addEntry(testEntry);
            }
        }
    }

    /*
     * Certificates are generally signed by a rootCA or an intermediate CA.
     * A certificate with expired validity will fail TLS handshake.
     *
     * Creating client cert with expired validity will verify both
     * certificate validity and server side MutualTLS.
     */
    @Test
    public void testExpiredCAIssuedClientCertificate() throws Exception {
        Date serverCertStartDate = new Date();
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate (expired validity)
        Date clientCertStartDate = new Date();
        clientCertStartDate = new Date(clientCertStartDate.getTime() - TimeUnit.HOURS.toMillis(1));
        Date clientCertEndDate = clientCertStartDate;
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        try {
            testClient(clientConf, numBookies);
            fail("ledger Write should have failed with expired certificate");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // valid
        }
    }


    /*
     * Host name on the certificate should be verified to protect against man-in-the-middle
     * attack.
     *
     * From RFC 6125, X509v3 Extended attributes Subject Alternative Name should be
     * used to verify host identity. If SubAltName does not exist, then Subject CN should be
     * used to identify host.
     */
    @Test
    public void testCertificateWithBadClientIdentity() throws Exception {
        Date serverCertStartDate = new Date();
        // end time is now + 1hr
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, "1.1.1.1", dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate (expired validity)
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        // restart bookies
        restartBookies();

        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert, rootCACert});

        try {
            testClient(clientConf, numBookies);
            fail("ledger Write should have failed with wrong CN");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // valid
        }
    }

    /*
     * To authenticate each others certificates, TLS endpoints use trust chain. If
     * the certificate is either not signed by trusted source or the trust chain
     * is incomplete, the certificate cannot be trusted and should result in failure
     * of mutual authentication.
     */
    @Test
    public void testIncompleteTrustChain() throws Exception {
        Date serverCertStartDate = new Date();
        Date serverCertEndDate = new Date(serverCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();
        X509Certificate serverCertificate = createSignedCertificate(intermediateCAPrivateKey, serverKeyPair.getPublic(),
                serverCertStartDate, serverCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // client certificate
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(serverCertificate);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert});

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        // incomplete trust chain on client
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {rootCACert});

        try {
            testClient(clientConf, numBookies);
            fail("ledger Write should have failed due to incomplete trust chain on client");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // valid
        }
    }

    /*
     * On a key and certificate mismatch, TLS handshake fails as server will be
     * unable to create a SSL context.
     */
    @Test
    public void testKeyCertificateMismatch() throws Exception {
        final String dn = "CN=" + InetAddress.getLocalHost().getHostName();

        KeyPair serverKeyPair = generateKeyPair();

        // client certificate
        Date clientCertStartDate = new Date();
        Date clientCertEndDate = new Date(clientCertStartDate.getTime() + TimeUnit.HOURS.toMillis(1));
        KeyPair clientKeyPair = generateKeyPair();
        X509Certificate clientCertificate = createSignedCertificate(intermediateCAPrivateKey, clientKeyPair.getPublic(),
                clientCertStartDate, clientCertEndDate, null, dn, intermediateCACert.getSubjectDN().toString(),
                ALGOSHA1WITHRSA);

        // mock server
        spy(TLSUtils.class);
        // mismatch server certificate (replacing server cert with intermediate cert)
        when(TLSUtils.getCertificate(baseConf.getTLSCertificatePath()))
                .thenReturn(intermediateCACert);
        when(TLSUtils.getPrivateKey(baseConf.getTLSKeyStore(), null))
                .thenReturn(serverKeyPair.getPrivate());
        when(TLSUtils.getTrustChain(baseConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {intermediateCACert});

        SecurityHandlerFactory sslConnectionFactory = SecurityProviderFactoryFactory
                .getSecurityProviderFactory(baseConf.getTLSProviderFactoryClass());
        try {
            sslConnectionFactory.init(SecurityHandlerFactory.NodeType.Server, baseConf);
            sslConnectionFactory.newTLSHandler();
        } catch (SecurityException se) {
            //valid
        } catch (Exception e) {
            fail("Expected to see a Security Exception due to mismatch in key and certificate");
        }

        // restart bookies
        restartBookies();

        // mock client
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf);
        when(TLSUtils.getCertificate(baseClientConf.getTLSCertificatePath()))
                .thenReturn(clientCertificate);
        when(TLSUtils.getPrivateKey(baseClientConf.getTLSKeyStore(), null))
                .thenReturn(clientKeyPair.getPrivate());
        // incomplete trust chain on client
        when(TLSUtils.getTrustChain(baseClientConf.getTLSTrustStore()))
                .thenReturn(new X509Certificate[] {rootCACert});

        try {
            testClient(clientConf, numBookies);
            fail("ledger Write should have failed due to key and cert mismatch on server");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // valid
        }
    }
}
