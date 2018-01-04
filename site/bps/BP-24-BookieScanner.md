---
title: "BP-24: BookieScanner: Enhance Data Integrity"
issue: https://github.com/apache/bookkeeper/issues/942
state: "Under Discussion"
release: "N/A"
---


### Motivation


Currently Bookie can't deal entry losing gracefully, the AutoRecovery is restricted to the bookie level, which means the AutoRecovery takes effect only after bookie is down. However when a disk fails, either or both the ledger index files and entry log files could potentially become corrupt. BookKeeper needs to provide mechanisms to identify and handle these problems.


### Proposed Changes


We introduce Bookie Scanner, which is a background task, to scan index files and entry log files to detect possible corruptions. Since data corruption may happen at any time on any block on any Bookie, it is important to identify these errors in a timely manner. This way, the bookie can remove/compact corrupted entries and re-replicate entries from other replicas, to maintain data integrity and reduce client errors. 


The Bookie Scanner needs to detect and cover following conditions:


- a ledger is missing local (no index file found for a given ledger), we can do this by looking into the ledger metadata.
- a ledger exists, but some entries are missing (no index entries found in the index file), we can check fragment’s metadata to verify this.
- a ledger exists, entries are found in index file, but the entries in entry log files are corrupted, we can use entry’s checksum to verify this.


A Bookie Scanner is integrated and run as part of compaction thread which already scans the entry log files.


#### Suspicious List


Besides regular scan, the scanner also maintains a list of suspicious ledgers and a list of suspicious entry log files. These are the ledgers / entry log files that caused specific types of exceptions to be thrown when entries are read from disk. The suspicious lists take priority over the regular ledgers and entry log files during scans. Moreover, the scanner should track which suspicious ledgers and entry log files it has scanned in the past x minutes, to avoid repeatedly scanning the same suspicious ledgers and entry log files.


The mechanism bookie scanner to decide which ledgers and entry log files to scan is as follows:


* When a bookie is serving read requests, if an IOException is caught, then the entry log file is marked as suspicious and added to the scanner’s suspicious entry log list, if a NoSuchLedger or NoSuchEntry exception is caught, then the given ledger is marked as suspicious and added to the scanner’s suspicious ledger list.
* The bookie scanner loops over all the ledger index files. At each iteration, it checks one ledger.
   * If the suspicious ledger list is not empty, it pops one suspicious ledger to scan
* The bookie scanner loops over all the entry log files. At each iteration, it checks one entry log file.
   * If the suspicious entry log list is not empty, it popos one suspicious entry log to scan




#### Scan Cursor


To keep track of the scanning position among the ledgers and entry log files, a cursor is maintained. The cursor is saved to disk periodically (it should be configurable). This way, even the bookie process restarts or the server reboots, the scan doesn’t have to restart from the very beginning.


#### Scan Throttling


The scanner is I/O consumption. We cannot afford to loop scanning them continuously, because this could create busy I/Os and harm normal I/O performance. Instead, the scanners run at a configured rate for throttling purpose (similar as throttling at compaction), with appropriate sleep intervals between two scan periods. When a ledger or an entry log file is marked as suspicious, the bookie scanner is woken up if it is waiting for the next scan period.


Some pseudocode to show the callable example:


BookieScanner scanner = new BookieScanner();
scanner.scanLedgers();// find the missing ledgers, and check whether entries are missing, for condition1 and condition2
scanner.scanEntryLogs();// check whether the entries in the entry log are corrupted, for condition 3 
scanner.scanSuspiciousLedgers();
scanner.scanSuspiciousEntryLogs();
scanner.fixCorrupt();//replicate lost fragments from replica




### Compatibility, Deprecation, and Migration Plan


- This new feature has no impacts to existing users.




### Test Plan


After we delete some entry logs which contain some ledgers' data, we will get NoSuchEntry or NoSuchLedgerthe Exception, and the BookieScanner will be triggered to recovery all the data from replica. After a period, we can check that by readEntry from the bookie. Addition, we can check BookieScanner’s ability to recovery index files using similar way.


### Rejected Alternatives


We can achieve the same stuff by adding one dedicated thread to check the ledger's state and scan the index files and entry logs to verify entry's integrity regularly, but that could lead much high overhead. And the actions needed by this task just part of compacting task, such as scanning the entry log, so we can integrate BookieScanner to it.