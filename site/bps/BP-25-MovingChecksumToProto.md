---
title: "BP-25: MovingChecksumToProto--Refactor the checksum part of bookkeeper"
issue: https://github.com/apache/bookkeeper/issues/1007
state: "Adopted"
release: "4.7.0"
---

### Motivation

Current the checksum implementation is in client module while the checksum semantic is more close to protocol. Moreover, moving the checksum implementation to protocol will avoid server module's dependency to client module when doing checksum in server side.

### Public Interfaces

An internal refactor not affecting public interfaces.

### Proposed Changes

Move the DigestManager and related classes to proto module

### Compatibility, Deprecation, and Migration Plan
N/A

### Test Plan
The original all tests should work as before.

### Rejected Alternatives
N/A
