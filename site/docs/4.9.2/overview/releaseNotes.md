---
title: Apache BookKeeper 4.9.2 Release Notes
---

This is the 18th release of Apache BookKeeper!

The 4.9.2 release incorporates a few critical bug fixes, since previous major release, 4.9.0.

Apache BookKeeper/DistributedLog users are encouraged to [upgrade to 4.9.2](../../admin/upgrade). The technical details of
this release are summarized below.

### Dependencies Changes

No dependency change.

### Bug Fixes

- [Issue #1973: [DLOG] Avoid double read in readahead](https://github.com/apache/bookkeeper/pull/1973)
- [Issue #1952: Filter empty string for networkTopologyScriptFileName](https://github.com/apache/bookkeeper/pull/1952)
- [Issue #1950: putEntryOffset translate FileInfoDeletedException](https://github.com/apache/bookkeeper/pull/1950)

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?q=label%3Arelease%2F4.9.1+is%3Aclosed)
