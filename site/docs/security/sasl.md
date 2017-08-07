---
title: Authentication using SASL
prev: ../tls
next: ../zookeeper
---

Bookies support client authentication via SASL. Currently we only support GSSAPI (Kerberos). We will start
with a general description of how to configure `SASL` for bookies, clients and autorecovery daemons, followed
by mechanism-specific details and wrap up with some operational details.

## SASL configuration for Bookies

1. Select the mechanisms to enable in the bookies. `GSSAPI` is the only mechanism currently supported by BookKeeper.
2. Add a `JAAS` config file for the selected mechanisms as described in the examples for setting up [GSSAPI (Kerberos)](#kerberos).
3. Pass the `JAAS` config file location as JVM parameter to each Bookie. For example:

    ```shell
    -Djava.security.auth.login.config=/etc/bookkeeper/bookie_jaas.conf 
    ```

4. Enable SASL auth plugin in bookies, by setting `bookieAuthProviderFactoryClass` to `org.apache.bookkeeper.sasl.SASLBookieAuthProviderFactory`.


    ```shell
    bookieAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLBookieAuthProviderFactory
    ```

5. If you are running `autorecovery` along with bookies, then you want to enable SASL auth plugin for `autorecovery`, by setting
    `clientAuthProviderFactoryClass` to `org.apache.bookkeeper.sasl.SASLClientProviderFactory`.

    ```shell
    clientAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLClientProviderFactory
    ```

6. Follow the steps in [GSSAPI (Kerberos)](#kerberos) to configure SASL.

#### <a name="notes"></a> Important Notes

1. `Bookie` is a section name in the JAAS file used by each bookie. This section tells the bookie which principal to use
    and the location of the keytab where the principal is stored. It allows the bookie to login using the keytab specified in this section.
2. `Auditor` is a section name in the JASS file used by `autorecovery` daemon (it can be co-run with bookies). This section tells the
    `autorecovery` daemon which principal to use and the location of the keytab where the principal is stored. It allows the bookie to
    login using the keytab specified in this section.
3. The `Client` section is used to authenticate a SASL connection with ZooKeeper. It also allows the bookies to set ACLs on ZooKeeper nodes
    which locks these nodes down so that only the bookies can modify it. It is necessary to have the same primary name across all bookies.
    If you want to use a section name other than `Client`, set the system property `zookeeper.sasl.client` to the appropriate name
    (e.g `-Dzookeeper.sasl.client=ZKClient`).
4. ZooKeeper uses `zookeeper` as the service name by default. If you want to change this, set the system property
    `zookeeper.sasl.client.username` to the appropriate name (e.g. `-Dzookeeper.sasl.client.username=zk`).

## SASL configuration for Clients

To configure `SASL` authentication on the clients:

1. Select a `SASL` mechanism for authentication and add a `JAAS` config file for the selected mechanism as described in the examples for
    setting up [GSSAPI (Kerberos)](#kerberos).
2. Pass the `JAAS` config file location as JVM parameter to each client JVM. For example:

    ```shell
    -Djava.security.auth.login.config=/etc/bookkeeper/bookkeeper_jaas.conf 
    ```

3. Configure the following properties in bookkeeper `ClientConfiguration`:

    ```shell
    clientAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLClientProviderFactory
    ```

Follow the steps in [GSSAPI (Kerberos)](#kerberos) to configure SASL for the selected mechanism.

## <a name="kerberos"></a> Authentication using SASL/Kerberos

### Prerequisites

#### Kerberos

If your organization is already using a Kerberos server (for example, by using `Active Directory`), there is no need to
install a new server just for BookKeeper. Otherwise you will need to install one, your Linux vendor likely has packages
for `Kerberos` and a short guide on how to install and configure it ([Ubuntu](https://help.ubuntu.com/community/Kerberos),
[Redhat](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html)).
Note that if you are using Oracle Java, you will need to download JCE policy files for your Java version and copy them to `$JAVA_HOME/jre/lib/security`.

#### Kerberos Principals

If you are using the organization’s Kerberos or Active Directory server, ask your Kerberos administrator for a principal
for each Bookie in your cluster and for every operating system user that will access BookKeeper with Kerberos authentication
(via clients and tools).

If you have installed your own Kerberos, you will need to create these principals yourself using the following commands:

```shell
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey bookkeeper/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{keytabname}.keytab bookkeeper/{hostname}@{REALM}"
```

##### All hosts must be reachable using hostnames

It is a *Kerberos* requirement that all your hosts can be resolved with their FQDNs.

### Configuring Bookies

1. Add a suitably modified JAAS file similar to the one below to each Bookie’s config directory, let’s call it `bookie_jaas.conf`
for this example (note that each bookie should have its own keytab):

    ```
    Bookie {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/etc/security/keytabs/bookie.keytab"
        principal="bookkeeper/bk1.hostname.com@EXAMPLE.COM";
    };
    // ZooKeeper client authentication
    Client {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/etc/security/keytabs/bookie.keytab"
        principal="bookkeeper/bk1.hostname.com@EXAMPLE.COM";
    };
    // If you are running `autorecovery` along with bookies
    Auditor {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/etc/security/keytabs/bookie.keytab"
        principal="bookkeeper/bk1.hostname.com@EXAMPLE.COM";
    };
    ```

    The `Bookie` section in the JAAS file tells the bookie which principal to use and the location of the keytab where this principal is stored.
    It allows the bookie to login using the keytab specified in this section. See [notes](#notes) for more details on Zookeeper’s SASL configuration.

2. Pass the name of the JAAS file as a JVM parameter to each Bookie:

    ```shell
    -Djava.security.auth.login.config=/etc/bookkeeper/bookie_jaas.conf
    ```

    You may also wish to specify the path to the `krb5.conf` file
    (see [JDK’s Kerberos Requirements](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details):

    ```shell
    -Djava.security.krb5.conf=/etc/bookkeeper/krb5.conf
    ```

3. Make sure the keytabs configured in the JAAS file are readable by the operating system user who is starting the Bookies.

4. Enable SASL authentication plugin in the bookies by setting following parameters.

    ```shell
    bookieAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLBookieAuthProviderFactory
    # if you run `autorecovery` along with bookies
    clientAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLClientProviderFactory
    ```

### Configuring Clients

To configure SASL authentication on the clients:

1. Clients will authenticate to the cluster with their own principal (usually with the same name as the user running the client),
    so obtain or create these principals as needed. Then create a `JAAS` file for each principal. The `BookKeeper` section describes
    how the clients like writers and readers can connect to the Bookies. The following is an example configuration for a client using
    a keytab (recommended for long-running processes):

    ```
    BookKeeper {
        com.sun.security.auth.module.Krb5LoginModule required
        useKeyTab=true
        storeKey=true
        keyTab="/etc/security/keytabs/bookkeeper.keytab"
        principal="bookkeeper-client-1@EXAMPLE.COM";
    };
    ```


2. Pass the name of the JAAS file as a JVM parameter to the client JVM:

    ```shell
    -Djava.security.auth.login.config=/etc/bookkeeper/bookkeeper_jaas.conf
    ```

    You may also wish to specify the path to the `krb5.conf` file (see
    [JDK’s Kerberos Requirements](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details).

    ```shell
    -Djava.security.krb5.conf=/etc/bookkeeper/krb5.conf
    ```


3. Make sure the keytabs configured in the `bookkeeper_jaas.conf` are readable by the operating system user who is starting bookkeeper client.

4. Enable SASL authentication plugin in the client by setting following parameters.

    ```shell
    clientAuthProviderFactoryClass=org.apache.bookkeeper.sasl.SASLClientProviderFactory
    ```

## Enabling Logging for SASL

To enable SASL debug output, you can set `sun.security.krb5.debug` system property to `true`.

