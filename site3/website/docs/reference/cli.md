---
id: cli
title: BookKeeper CLI tool reference
---
## `bookkeeper` command

Manages bookies.


#### Environment variables

| Environment variable     | Description                                                                                          | Default                                                   | 
|--------------------------|------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `BOOKIE_LOG_CONF`        | The Log4j configuration file.                                                                        | `${bookkeeperHome}/bookkeeper-server/conf/log4j2.xml`     | 
| `BOOKIE_CONF`            | The configuration file for the bookie.                                                               | `${bookkeeperHome}/bookkeeper-server/conf/bk_server.conf` | 
| `BOOKIE_EXTRA_CLASSPATH` | Extra paths to add to BookKeeper's [classpath](https://en.wikipedia.org/wiki/Classpath_(Java)).      |                                                           | 
| `BOOKIE_PID_DIR`         | The directory where the bookie server PID file is stored.                                            |                                                           | 
| `BOOKIE_STOP_TIMEOUT`    | The wait time before forcefully killing the bookie server instance if stopping it is not successful. |                                                           | 

#### Commands



### bookie {#bookkeeper-shell-bookie}

Starts up a bookie.

##### Usage

```shell
$ bin/bookkeeper bookie
```

### localbookie {#bookkeeper-shell-localbookie}

Starts up an ensemble of N bookies in a single JVM process. Typically used for local experimentation and development.

##### Usage

```shell
$ bin/bookkeeper localbookie \ 
	N
```

### autorecovery {#bookkeeper-shell-autorecovery}

Runs the autorecovery service.

##### Usage

```shell
$ bin/bookkeeper autorecovery
```

### upgrade {#bookkeeper-shell-upgrade}

Upgrades the bookie's filesystem.

##### Usage

```shell
$ bin/bookkeeper upgrade \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| --upgrade | Upgrade the filesystem. | 
| --rollback | Rollback the filesystem to a previous version. | 
| --finalize | Mark the upgrade as complete. | 


### shell {#bookkeeper-shell-shell}

Runs the bookie's shell for admin commands.

##### Usage

```shell
$ bin/bookkeeper shell
```

### help {#bookkeeper-shell-help}

Displays the help message for the `bookkeeper` tool.

##### Usage

```shell
$ bin/bookkeeper help
```




## BookKeeper shell



### queryautorecoverystatus {#bookkeeper-shell-queryautorecoverystatus}

Query the autorecovery status

##### Usage

```shell
$ bin/bookkeeper shell queryautorecoverystatus
```

| Flag           | Description |
|----------------| ----------- |
| -v,--verbose   | List recovering detailed ledger info | 

### autorecovery {#bookkeeper-shell-autorecovery}

Enable or disable autorecovery in the cluster.

##### Usage

```shell
$ bin/bookkeeper shell autorecovery \ 
	<options>
```

| Flag         | Description |
|--------------| ----------- |
| -e,--enable  | Enable autorecovery of underreplicated ledgers | 
| -d,--disable | Disable autorecovery of underreplicated ledgers | 


### bookieformat {#bookkeeper-shell-bookieformat}

Format the current server contents.

##### Usage

```shell
$ bin/bookkeeper shell bookieformat \ 
	<options>
```

| Flag                | Description |
|---------------------| ----------- |
| -n,--nonInteractive | Whether to confirm if old data exists. | 
| -f,--force          | If [nonInteractive] is specified, then whether to force delete the old data without prompt..? | 
| -d,--deleteCookie   | Delete its cookie on zookeeper | 


### initbookie {#bookkeeper-shell-initbookie}

Initialize new bookie, by making sure that the journalDir, ledgerDirs and
indexDirs are empty and there is no registered Bookie with this BookieId.

If there is data present in current bookie server, the init operation will fail. If you want to format
the bookie server, use `bookieformat`.


##### Usage

```shell
$ bin/bookkeeper shell initbookie
```

### bookieinfo {#bookkeeper-shell-bookieinfo}

Retrieve bookie info such as free and total disk space.

##### Usage

```shell
$ bin/bookkeeper shell bookieinfo
```

### bookiesanity {#bookkeeper-shell-bookiesanity}

Sanity test for local bookie. Create ledger and write/read entries on the local bookie.

##### Usage

```shell
$ bin/bookkeeper shell bookiesanity \ 
	<options>
```

| Flag           | Description |
|----------------| ----------- |
| -e,--entries N | Total entries to be added for the test (default 10) | 
| -t,--timeout N | Timeout for write/read operations in seconds (default 1) | 


### decommissionbookie {#bookkeeper-shell-decommissionbookie}

Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie are replicated.

##### Usage

```shell
$ bin/bookkeeper shell decommissionbookie
```

### deleteledger {#bookkeeper-shell-deleteledger}

Delete a ledger

##### Usage

```shell
$ bin/bookkeeper shell deleteledger \ 
	<options>
```

| Flag                    | Description |
|-------------------------| ----------- |
| -l,--ledgerid LEDGER_ID | Ledger ID | 
| -f,--force              | Whether to force delete the Ledger without prompt..? | 


### endpointinfo {#bookkeeper-shell-endpointinfo}

Get endpoints of a Bookie.

##### Usage

```shell
$ bin/bookkeeper shell endpointinfo
```

### expandstorage {#bookkeeper-shell-expandstorage}

Add new empty ledger/index directories. Update the directories info in the conf file before running the command.

##### Usage

```shell
$ bin/bookkeeper shell expandstorage
```

### help {#bookkeeper-shell-help}

Displays the help message.

##### Usage

```shell
$ bin/bookkeeper shell help
```

### lastmark {#bookkeeper-shell-lastmark}

Print last log marker.

##### Usage

```shell
$ bin/bookkeeper shell lastmark
```

### ledger {#bookkeeper-shell-ledger}

Dump ledger index entries into readable format.

##### Usage

```shell
$ bin/bookkeeper shell ledger \ 
	<options>
```

| Flag                | Description |
|---------------------| ----------- |
| -m,--meta LEDGER_ID | Print meta information | 


### ledgermetadata {#bookkeeper-shell-ledgermetadata}

Print the metadata for a ledger.

##### Usage

```shell
$ bin/bookkeeper shell ledgermetadata \ 
	<options>
```

| Flag                    | Description |
|-------------------------| ----------- |
| -l,--ledgerid LEDGER_ID | Ledger ID | 
 | --dump-to-file FILENAME | Dump metadata for ledger, to a file |
 | --restore-from-file FILENAME | Restore metadata for ledger, from a file | 


### listbookies {#bookkeeper-shell-listbookies}

List the bookies, which are running as either readwrite or readonly mode.

##### Usage

```shell
$ bin/bookkeeper shell listbookies \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -a,--all | Print all bookies | 
| -rw,--readwrite | Print readwrite bookies | 
| -ro,--readonly | Print readonly bookies | 
| -h,--hostnames | Also print hostname of the bookie | 


### listfilesondisc {#bookkeeper-shell-listfilesondisc}

List the files in JournalDirectory/LedgerDirectories/IndexDirectories.

##### Usage

```shell
$ bin/bookkeeper shell listfilesondisc \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -txn,--journal | Print list of journal files | 
| -log,--entrylog | Print list of entryLog files | 
| -idx,--index | Print list of index files | 


### listledgers {#bookkeeper-shell-listledgers}

List all ledgers in the cluster (this may take a long time).

##### Usage

```shell
$ bin/bookkeeper shell listledgers \ 
	<options>
```

| Flag                | Description |
|---------------------| ----------- |
| -bookieid BOOKIE_ID | List ledgers residing in this bookie | 
| -m,--meta           | Print metadata | 


### listunderreplicated {#bookkeeper-shell-listunderreplicated}

List ledgers marked as underreplicated, with optional options to specify missing replica (BookieId) and to exclude missing replica.

##### Usage

```shell
$ bin/bookkeeper shell listunderreplicated \ 
	<options>
```

| Flag                        | Description |
|-----------------------------| ----------- |
| -missingreplica BOOKIE_ADDRESS | Bookie Id of missing replica | 
| -excludingmissingreplica BOOKIE_ADDRESS | Bookie Id of missing replica to ignore | 
| -printmissingreplica        | Whether to print missingreplicas list? | 
| -printreplicationworkerid   | Whether to print replicationworkerid? | 
| -c,--onlydisplayledgercount | Only display underreplicated ledger count | 


### metaformat {#bookkeeper-shell-metaformat}

Format Bookkeeper metadata in Zookeeper. This command is deprecated since 4.7.0,
in favor of using `initnewcluster` for initializing a new cluster and `nukeexistingcluster` for nuking an existing cluster.


##### Usage

```shell
$ bin/bookkeeper shell metaformat \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -n,--nonInteractive | Whether to confirm if old data exists..? | 
| -f,--force | If [nonInteractive] is specified, then whether to force delete the old data without prompt. | 


### initnewcluster {#bookkeeper-shell-initnewcluster}

Initializes a new bookkeeper cluster. If initnewcluster fails then try nuking
existing cluster by running nukeexistingcluster before running initnewcluster again


##### Usage

```shell
$ bin/bookkeeper shell initnewcluster
```

### nukeexistingcluster {#bookkeeper-shell-nukeexistingcluster}

Nuke bookkeeper cluster by deleting metadata

##### Usage

```shell
$ bin/bookkeeper shell nukeexistingcluster \ 
	<options>
```

| Flag                           | Description |
|--------------------------------| ----------- |
| -p,--zkledgersrootpath ZK_LEDGER_ROOT_PATH | zookeeper ledgers rootpath | 
| -i,--instanceid INSTANCE_ID    | instance id | 
| -f,--force                     | If instanceid is not specified, then whether to force nuke the metadata without validating instanceid | 


### lostbookierecoverydelay {#bookkeeper-shell-lostbookierecoverydelay}

Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper.

##### Usage

```shell
$ bin/bookkeeper shell lostbookierecoverydelay \ 
	<options>
```

| Flag           | Description |
|----------------| ----------- |
| -g,--get       | Get LostBookieRecoveryDelay value (in seconds) | 
| -s,--set VALUE | Set LostBookieRecoveryDelay value (in seconds) | 


### readjournal {#bookkeeper-shell-readjournal}

Scan a journal file and format the entries into readable format.

##### Usage

```shell
$ bin/bookkeeper shell readjournal \ 
	<options>
```

| Flag                                            | Description |
|-------------------------------------------------|--|
| -m,--msg                                        | Print message body | 
| -dir JOURNAL_ID or JOURNAL_FILE_NAME | Journal directory (needed if more than one journal configured) | 


### readledger {#bookkeeper-shell-readledger}

Read a range of entries from a ledger.

##### Usage

```shell
$ bin/bookkeeper shell readledger \ 
	<ledger_id> [<start_entry_id> [<end_entry_id>]]
```

| Flag                  | Description                                  |
|-----------------------|----------------------------------------------|
| -m,--msg              | Print message body                           | 
| -l,--ledgerid LEDGER_ID | Ledger ID                                    | 
| -fe,--firstentryid ENTRY_ID | First EntryID                                | 
| -le,--lastentryid ENTRY_ID | Last EntryID                                 | 
| -b,--bookie BOOKIE_ID | Only read from a specific bookie             | 
| -r,--force-recovery   | Ensure the ledger is properly closed before reading | 


### readlog {#bookkeeper-shell-readlog}

Scan an entry file and format the entries into readable format.

##### Usage

```shell
$ bin/bookkeeper shell readlog \ 
	<entry_log_id | entry_log_file_name> \ 
	<options>
```

| Flag                                | Description |
|-------------------------------------| ----------- |
| -m,--msg                            | Print message body | 
| -l,--ledgerid LEDGER_ID             | Ledger ID | 
| -e,--entryid ENTRY_ID               | Entry ID | 
| -sp,--startpos START_ENTRY_LOG_BYTE_POS | Start Position | 
| -ep,--endpos END_ENTRY_LOG_BYTE_POS | End Position | 


### recover {#bookkeeper-shell-recover}

Recover the ledger data for failed bookie.

##### Usage

```shell
$ bin/bookkeeper shell recover \ 
	<bookieSrc[,bookieSrc,...]> \ 
	<options>
```

| Flag                  | Description                                  |
|-----------------------|----------------------------------------------|
| -d,--deleteCookie     | Delete cookie node for the bookie.           | 
| -dr,--dryrun          | Printing the recovery plan w/o doing actual recovery | 
| -f,--force            | Force recovery without confirmation          | 
| -l,--ledger LEDGER_ID | Recover a specific ledger                    | 
| -q,--query            | Query the ledgers that contain given bookies | 
| -sk,--skipOpenLedgers | Skip recovering open ledgers                 | 
| -sku,--skipUnrecoverableLedgers | Skip unrecoverable ledgers                   | 


### simpletest {#bookkeeper-shell-simpletest}

Simple test to create a ledger and write entries to it.

##### Usage

```shell
$ bin/bookkeeper shell simpletest \ 
	<options>
```

| Flag             | Description |
|------------------| ----------- |
| -e,--ensemble N  | Ensemble size (default 3) | 
| -w,--writeQuorum N | Write quorum size (default 2) | 
| -a,--ackQuorum N | Ack quorum size (default 2) | 
| -n,--numEntries N | Entries to write (default 1000) | 


### triggeraudit {#bookkeeper-shell-triggeraudit}

Force trigger the Audit by resetting the lostBookieRecoveryDelay.

##### Usage

```shell
$ bin/bookkeeper shell triggeraudit
```

### updatecookie {#bookkeeper-shell-updatecookie}

Update bookie id in cookie.

##### Usage

```shell
$ bin/bookkeeper shell updatecookie \ 
	<options>
```

| Flag                                  | Description |
|---------------------------------------|--|
| -b,--bookieId HOSTNAME or IP | Bookie Id | 
| -d,--delete FORCE              | Delete cookie both locally and in ZooKeeper | 
| -e,--expandstorage                    | Expand Storage | 
| -l,--list                             | List paths of all the cookies present locally and on zookkeeper | 


### updateledgers {#bookkeeper-shell-updateledgers}

Update bookie id in ledgers (this may take a long time).

##### Usage

```shell
$ bin/bookkeeper shell updateledgers \ 
	<options>
```

| Flag                         | Description                                                                       |
|------------------------------|-----------------------------------------------------------------------------------|
| -b,--bookieId HOSTNAME or IP | Bookie Id                                                                         | 
| -s,--updatespersec           | Number of ledgers updating per second (default 5 per sec)                         | 
| -l,--limit N                 | Maximum number of ledgers to update (default no limit)                            | 
| -v,--verbose                 | Print status of the ledger updation (default false)                               | 
| -p,--printprogress N         | Print messages on every configured seconds if verbose turned on (default 10 secs) | 
| -r,--maxOutstandingReads N   | Max outstanding reads (default: 5 * updatespersec)                                |

### updateBookieInLedger {#bookkeeper-shell-updateBookieInLedger}

Replace srcBookie with destBookie in ledger metadata. (this may take a long time).
Useful when Host-reip or data-migration. In that case, shutdown bookie process in src-bookie,
use this command to update ledger metadata by replacing src-bookie to dest-bookie where data has been copied/moved.
Start the bookie process on dest-bookie and dest-bookie will serve copied ledger data from src-bookie.


##### Usage

```shell
$ bin/bookkeeper shell updateBookieInLedger \ 
	<options>
```

| Flag                      | Description |
|---------------------------| ----------- |
| -sb,--srcBookie BOOKIE_ID | Source Bookie Id | 
| -db,--destBookie BOOKIE_ID     | Destination Bookie Id | 
| -s,--updatespersec N          | Number of ledgers updating per second (default 5 per sec) | 
| -l,--limit N              | Maximum number of ledgers to update (default no limit) | 
| -v,--verbose                  | Print status of the ledger updation (default false) | 
| -p,--printprogress N      | Print messages on every configured seconds if verbose turned on (default 10 secs) | 
| -r,--maxOutstandingReads N | Max outstanding reads (default: 5 * updatespersec)                                |

### whoisauditor {#bookkeeper-shell-whoisauditor}

Print the node which holds the auditor lock

##### Usage

```shell
$ bin/bookkeeper shell whoisauditor
```

### whatisinstanceid {#bookkeeper-shell-whatisinstanceid}

Print the instanceid of the cluster

##### Usage

```shell
$ bin/bookkeeper shell whatisinstanceid
```

### convert-to-db-storage {#bookkeeper-shell-convert-to-db-storage}

Convert bookie indexes from InterleavedStorage to DbLedgerStorage format

##### Usage

```shell
$ bin/bookkeeper shell convert-to-db-storage
```

### convert-to-interleaved-storage {#bookkeeper-shell-convert-to-interleaved-storage}

Convert bookie indexes from DbLedgerStorage to InterleavedStorage format

##### Usage

```shell
$ bin/bookkeeper shell convert-to-interleaved-storage
```

### rebuild-db-ledger-locations-index {#bookkeeper-shell-rebuild-db-ledger-locations-index}

Rebuild DbLedgerStorage locations index

##### Usage

```shell
$ bin/bookkeeper shell rebuild-db-ledger-locations-index
```
