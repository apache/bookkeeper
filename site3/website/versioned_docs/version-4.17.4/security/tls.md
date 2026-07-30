---
id: tls
title: Encryption and Authentication using TLS
---

Apache BookKeeper allows clients and autorecovery daemons to communicate over TLS, although this is not enabled by default.

## Overview

The bookies need their own key and certificate in order to use TLS. Clients can optionally provide a key and a certificate
for mutual authentication.  Each bookie or client can also be configured with a truststore, which is used to
determine which certificates (bookie or client identities) to trust (authenticate).

The truststore can be configured in many ways. To understand the truststore, consider the following two examples:

1. the truststore contains one or many certificates;
2. it contains a certificate authority (CA).

In (1), with a list of certificates, the bookie or client will trust any certificate listed in the truststore.
In (2), with a CA, the bookie or client will trust any certificate that was signed by the CA in the truststore.

(TBD: benefits)

## Generate TLS key and certificate {#bookie-keystore}

The first step of deploying TLS is to generate the key and the certificate for each machine in the cluster.
You can use Java’s `keytool` utility to accomplish this task. We will generate the key into a temporary keystore
initially so that we can export and sign it later with CA.

```shell
keytool -keystore bookie.keystore.jks -alias localhost -validity {validity} -genkey
```

You need to specify two parameters in the above command:

1. `keystore`: the keystore file that stores the certificate. The *keystore* file contains the private key of
    the certificate; hence, it needs to be kept safely.
2. `validity`: the valid time of the certificate in days.

<div class="alert alert-success">
Ensure that common name (CN) matches exactly with the fully qualified domain name (FQDN) of the server.
The client compares the CN with the DNS domain name to ensure that it is indeed connecting to the desired server, not a malicious one.
</div>

## Creating your own CA

After the first step, each machine in the cluster has a public-private key pair, and a certificate to identify the machine.
The certificate, however, is unsigned, which means that an attacker can create such a certificate to pretend to be any machine.

Therefore, it is important to prevent forged certificates by signing them for each machine in the cluster.
A `certificate authority (CA)` is responsible for signing certificates. CA works likes a government that issues passports —
the government stamps (signs) each passport so that the passport becomes difficult to forge. Other governments verify the stamps
to ensure the passport is authentic. Similarly, the CA signs the certificates, and the cryptography guarantees that a signed
certificate is computationally difficult to forge. Thus, as long as the CA is a genuine and trusted authority, the clients have
high assurance that they are connecting to the authentic machines.

```shell
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
```

The generated CA is simply a *public-private* key pair and certificate, and it is intended to sign other certificates.

The next step is to add the generated CA to the clients' truststore so that the clients can trust this CA:

```shell
keytool -keystore bookie.truststore.jks -alias CARoot -import -file ca-cert
```

NOTE: If you configure the bookies to require client authentication by setting `sslClientAuthentication` to `true` on the
[bookie config](../reference/config), then you must also provide a truststore for the bookies and it should have all the CA
certificates that clients keys were signed by.

```shell
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
```

In contrast to the keystore, which stores each machine’s own identity, the truststore of a client stores all the certificates
that the client should trust. Importing a certificate into one’s truststore also means trusting all certificates that are signed
by that certificate. As the analogy above, trusting the government (CA) also means trusting all passports (certificates) that
it has issued. This attribute is called the chain of trust, and it is particularly useful when deploying TLS on a large BookKeeper cluster.
You can sign all certificates in the cluster with a single CA, and have all machines share the same truststore that trusts the CA.
That way all machines can authenticate all other machines.

## Signing the certificate

The next step is to sign all certificates in the keystore with the CA we generated. First, you need to export the certificate from the keystore:

```shell
keytool -keystore bookie.keystore.jks -alias localhost -certreq -file cert-file
```

Then sign it with the CA:

```shell
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days {validity} -CAcreateserial -passin pass:{ca-password}
```

Finally, you need to import both the certificate of the CA and the signed certificate into the keystore:

```shell
keytool -keystore bookie.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore bookie.keystore.jks -alias localhost -import -file cert-signed
```

The definitions of the parameters are the following:

1. `keystore`: the location of the keystore
2. `ca-cert`: the certificate of the CA
3. `ca-key`: the private key of the CA
4. `ca-password`: the passphrase of the CA
5. `cert-file`: the exported, unsigned certificate of the bookie
6. `cert-signed`: the signed certificate of the bookie

(TBD: add a script to automatically generate truststores and keystores.)

## Configuring Bookies

Bookies support TLS for connections on the same service port. In order to enable TLS, you need to configure `tlsProvider` to be either
`JDK` or `OpenSSL`. If `OpenSSL` is configured, it will use `netty-tcnative-boringssl-static`, which loads a corresponding binding according
to the platforms to run bookies.

> Current `OpenSSL` implementation doesn't depend on the system installed OpenSSL library. If you want to leverage the OpenSSL installed on
the system, you can check [this example](http://netty.io/wiki/forked-tomcat-native.html) on how to replaces the JARs on the classpath with
netty bindings to leverage installed OpenSSL.

The following TLS configs are needed on the bookie side:

```shell
tlsProvider=OpenSSL
tlsProviderFactoryClass=org.apache.bookkeeper.tls.TLSContextFactory
# key store
tlsKeyStoreType=JKS
tlsKeyStore=/var/private/tls/bookie.keystore.jks
tlsKeyStorePasswordPath=/var/private/tls/bookie.keystore.passwd
# trust store
tlsTrustStoreType=JKS
tlsTrustStore=/var/private/tls/bookie.truststore.jks
tlsTrustStorePasswordPath=/var/private/tls/bookie.truststore.passwd
```

NOTE: it is important to restrict access to the store files and corresponding password files via filesystem permissions.

Optional settings that are worth considering:

1. tlsClientAuthentication=false: Enable/Disable using TLS for authentication. This config when enabled will authenticate the other end
    of the communication channel. It should be enabled on both bookies and clients for mutual TLS.
2. tlsEnabledCipherSuites= A cipher suite is a named combination of authentication, encryption, MAC and key exchange
    algorithm used to negotiate the security settings for a network connection using TLS network protocol. By default,
    it is null. [OpenSSL Ciphers](https://www.openssl.org/docs/man1.0.2/man1/ciphers.html)
    [JDK Ciphers](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites)
3. tlsEnabledProtocols = TLSv1.2,TLSv1.1,TLSv1 (list out the TLS protocols that you are going to accept from clients).
    By default, it is not set.

To verify the bookie's keystore and truststore are setup correctly you can run the following command:

```shell
openssl s_client -debug -connect localhost:3181 -tls1
```

NOTE: TLSv1 should be listed under `tlsEnabledProtocols`.

In the output of this command you should see the server's certificate:

```shell
-----BEGIN CERTIFICATE-----
{variable sized random bytes}
-----END CERTIFICATE-----
```

If the certificate does not show up or if there are any other error messages then your keystore is not setup correctly.

## Configuring Clients

TLS is supported only for the new BookKeeper client (BookKeeper versions 4.5.0 and higher), the older clients are not
supported. The configs for TLS will be the same as bookies.

If client authentication is not required by the bookies, the following is a minimal configuration example:

```shell
tlsProvider=OpenSSL
tlsProviderFactoryClass=org.apache.bookkeeper.tls.TLSContextFactory
clientTrustStore=/var/private/tls/client.truststore.jks
clientTrustStorePasswordPath=/var/private/tls/client.truststore.passwd
```

If client authentication is required, then a keystore must be created for each client, and the bookies' truststores must
trust the certificate in the client's keystore. This may be done using commands that are similar to what we used for
the [bookie keystore](#bookie-keystore).

And the following must also be configured:

```shell
tlsClientAuthentication=true
clientKeyStore=/var/private/tls/client.keystore.jks
clientKeyStorePasswordPath=/var/private/tls/client.keystore.passwd
```

NOTE: it is important to restrict access to the store files and corresponding password files via filesystem permissions.

(TBD: add example to use tls in bin/bookkeeper script?)

## Enabling TLS Logging

You can enable TLS debug logging at the JVM level by starting the bookies and/or clients with `javax.net.debug` system property. For example:

```shell
-Djavax.net.debug=all
```

You can find more details on this in [Oracle documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html) on
[debugging SSL/TLS connections](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html).
