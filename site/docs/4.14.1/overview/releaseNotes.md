---
title: Apache BookKeeper 4.14.1 Release Notes
---

Release 4.14.1 fixes an issue with Prometheus metrics that was
found in 4.14.0.

Apache BookKeeper users are encouraged to upgrade to 4.14.1. 
The technical details of this release are summarized below.

### Bugs

- [https://github.com/apache/bookkeeper/pull/2718] Fix prometheus metric provider bug and add test to cover label scope

  After add label for prometheus metric by #2650, it will cause prometheus metric format check failed when no label specified for a statsLogger. The metric list as follow.


## Details

https://github.com/apache/bookkeeper/issues?q=+label%3Arelease%2F4.14.1
