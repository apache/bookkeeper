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


## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.2
