# BP-67: Support skipping compaction at busy times

### Motivation

Compaction mechanism is a critical part of the system. It is responsible for compacting the entry log file to reclaim disk space.
But it is an IO intensive operation. While compaction is running, it need to copy the valid data to a new file and delete the old file.
While the system is under heavy load, it is not a good idea to run compaction as it will consume more IO and CPU resources.
So, it is better to skip compaction at busy times. For many use cases, the busy time is predictable, we can skip compaction at those times.

### Configuration

add the following configuration:

```
skipCompactionHourRange
```

default value is empty, which means do not skip compaction.
Value format is `startHour-endHour`, for example, `0-6,18-24` means skip compaction from 0 to 6 and 18 to 24.


### Proposed Changes

Add a new configuration `skipCompactionHourRange` to control the time range of skipping compaction.


### Compatibility, Deprecation, and Migration Plan

Full compatibility with the existing behavior. No deprecation needed.

