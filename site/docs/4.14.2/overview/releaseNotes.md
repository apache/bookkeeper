---
title: Apache BookKeeper 4.14.2 Release Notes
---

Release 4.14.2 fixes an issue with Prometheus metrics that was
found in 4.14.0.

Apache BookKeeper users are encouraged to upgrade to 4.14.2. 
The technical details of this release are summarized below.

### Bugs

- [https://github.com/apache/bookkeeper/pull/2740] Fix Bouncy Castle fips incompatible issue

  In #2631, the default BouncyCastle was changed from non-fips into fips version. But the default version of BouncyCastle in Pulsar is the non-fips one(aimed to make it compatible with the old version of Pulsar).

  Bouncy Castle provides both FIPS and non-FIPS versions, but in a JVM, it can not include both of the 2 versions(non-Fips and Fips), and we have to exclude the current version before including the other. This makes the backward compatible a little hard, and that's why Pulsar has to involve an individual module for Bouncy Castle.

- [https://github.com/apache/bookkeeper/pull/2762] Upgrade libthrift to 0.14.2 to address multiple CVEs

  The current libthrift version 0.12.0 has multiple vulnerabilities: CVE-2019-0205 , CVE-2019-0210 , CVE-2020-13949

- [https://github.com/apache/bookkeeper/pull/2735] Exclude grpc-okhttp dependency

  The okhttp dependency version 2.7.4 is old and vulnerable. This dependency isn't needed and it causes Bookkeeper to be flagged for security vulnerabilities.

- [https://github.com/apache/bookkeeper/pull/2734] Upgrade Freebuilder version and fix the dependency

  - Freebuilder 1.14.9 contains an outdate jquery js file which causes the library to be flagged as vulnerable with the highest threat level in Sonatype IQ vulnerability scanner. This also flags Bookkeeper as vulnerable with the highest threat level although it is a false positive and not an actual threat.

  - Freebuilder shouldn't be exposed as a transitive dependency
    - it's an annotation processor which should be defined
      - [optional in maven](https://github.com/inferred/FreeBuilder#maven)
      - [compileOnly in gradle](https://github.com/inferred/FreeBuilder#gradle)

- [https://github.com/apache/bookkeeper/pull/2693] Upgrade vertx to 3.9.8, addresses CVE-2018-12541

  The current vertx version is 3.5.3 which has a vulnerability, CVE-2018-12541 .

## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.2
