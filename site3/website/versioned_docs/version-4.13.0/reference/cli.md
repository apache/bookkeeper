---
id: cli
title: BookKeeper CLI tool reference
---
## `bookkeeper` command

Manages bookies.


#### Environment variables

| Environment variable | Description | Default | 
| ------------------ | ----------- | ------ |
`BOOKIE_LOG_CONF` | The Log4j configuration file. | `${bookkeeperHome}/bookkeeper-server/conf/log4j.properties` | 
`BOOKIE_CONF` | The configuration file for the bookie. | `${bookkeeperHome}/bookkeeper-server/conf/bk_server.conf` | 
`BOOKIE_EXTRA_CLASSPATH` | Extra paths to add to BookKeeper's [classpath](https://en.wikipedia.org/wiki/Classpath_(Java)). |  | 
`ENTRY_FORMATTER_CLASS` | The entry formatter class used to format entries. |  | 
`BOOKIE_PID_DIR` | The directory where the bookie server PID file is stored. |  | 
`BOOKIE_STOP_TIMEOUT` | The wait time before forcefully killing the bookie server instance if stopping it is not successful. |  | 

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




### autorecovery {#bookkeeper-shell-autorecovery}

Enable or disable autorecovery in the cluster.

##### Usage

```shell
$ bin/bookkeeper shell autorecovery \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -enable | Enable autorecovery of underreplicated ledgers | 
| -disable | Disable autorecovery of underreplicated ledgers | 


### bookieformat {#bookkeeper-shell-bookieformat}

Format the current server contents.

##### Usage

```shell
$ bin/bookkeeper shell bookieformat \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -nonInteractive | Whether to confirm if old data exists. | 
| -force | If [nonInteractive] is specified, then whether to force delete the old data without prompt..? | 
| -deleteCookie | Delete its cookie on zookeeper | 


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

| Flag | Description |
| ---- | ----------- |
| -entries N | Total entries to be added for the test (default 10) | 
| -timeout N | Timeout for write/read operations in seconds (default 1) | 


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

| Flag | Description |
| ---- | ----------- |
| -ledgerid N | Ledger ID | 
| -force | Whether to force delete the Ledger without prompt..? | 


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

| Flag | Description |
| ---- | ----------- |
| -m LEDGER_ID | Print meta information | 


### ledgermetadata {#bookkeeper-shell-ledgermetadata}

Print the metadata for a ledger.

##### Usage

```shell
$ bin/bookkeeper shell ledgermetadata \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -ledgerid N | Ledger ID | 


### listbookies {#bookkeeper-shell-listbookies}

List the bookies, which are running as either readwrite or readonly mode.

##### Usage

```shell
$ bin/bookkeeper shell listbookies \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -readwrite | Print readwrite bookies | 
| -readonly | Print readonly bookies | 
| -hostnames | Also print hostname of the bookie | 


### listfilesondisc {#bookkeeper-shell-listfilesondisc}

List the files in JournalDirectory/LedgerDirectories/IndexDirectories.

##### Usage

```shell
$ bin/bookkeeper shell listfilesondisc \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -journal | Print list of journal files | 
| -entrylog | Print list of entryLog files | 
| -index | Print list of index files | 


### listledgers {#bookkeeper-shell-listledgers}

List all ledgers in the cluster (this may take a long time).

##### Usage

```shell
$ bin/bookkeeper shell listledgers \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -meta | Print metadata | 


### listunderreplicated {#bookkeeper-shell-listunderreplicated}

List ledgers marked as underreplicated, with optional options to specify missing replica (BookieId) and to exclude missing replica.

##### Usage

```shell
$ bin/bookkeeper shell listunderreplicated \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -missingreplica N | Bookie Id of missing replica | 
| -excludingmissingreplica N | Bookie Id of missing replica to ignore | 
| -printmissingreplica | Whether to print missingreplicas list? | 


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
| -nonInteractive | Whether to confirm if old data exists..? | 
| -force | If [nonInteractive] is specified, then whether to force delete the old data without prompt. | 


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

| Flag | Description |
| ---- | ----------- |
| -zkledgersrootpath | zookeeper ledgers rootpath | 
| -instanceid | instance id | 
| -force | If instanceid is not specified, then whether to force nuke the metadata without validating instanceid | 


### lostbookierecoverydelay {#bookkeeper-shell-lostbookierecoverydelay}

Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper.

##### Usage

```shell
$ bin/bookkeeper shell lostbookierecoverydelay \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -get | Get LostBookieRecoveryDelay value (in seconds) | 
| -set N | Set LostBookieRecoveryDelay value (in seconds) | 


### readjournal {#bookkeeper-shell-readjournal}

Scan a journal file and format the entries into readable format.

##### Usage

```shell
$ bin/bookkeeper shell readjournal \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -msg JOURNAL_ID|JOURNAL_FILENAME | Print message body | 
| -dir | Journal directory (needed if more than one journal configured) | 


### readledger {#bookkeeper-shell-readledger}

Read a range of entries from a ledger.

##### Usage

```shell
$ bin/bookkeeper shell readledger \ 
	<ledger_id> [<start_entry_id> [<end_entry_id>]]
```

### readlog {#bookkeeper-shell-readlog}

Scan an entry file and format the entries into readable format.

##### Usage

```shell
$ bin/bookkeeper shell readlog \ 
	<entry_log_id | entry_log_file_name> \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -msg | Print message body | 
| -ledgerid N | Ledger ID | 
| -entryid N | Entry ID | 
| -startpos N | Start Position | 
| -endpos | End Position | 


### recover {#bookkeeper-shell-recover}

Recover the ledger data for failed bookie.

##### Usage

```shell
$ bin/bookkeeper shell recover \ 
	<bookieSrc[,bookieSrc,...]> \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -deleteCookie | Delete cookie node for the bookie. | 


### simpletest {#bookkeeper-shell-simpletest}

Simple test to create a ledger and write entries to it.

##### Usage

```shell
$ bin/bookkeeper shell simpletest \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -ensemble N | Ensemble size (default 3) | 
| -writeQuorum N | Write quorum size (default 2) | 
| ackQuorum N | Ack quorum size (default 2) | 
| -numEntries N | Entries to write (default 1000) | 


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

| Flag | Description |
| ---- | ----------- |
| -bookieId <hostname|ip> | Bookie Id | 


### updateledgers {#bookkeeper-shell-updateledgers}

Update bookie id in ledgers (this may take a long time).

##### Usage

```shell
$ bin/bookkeeper shell updateledgers \ 
	<options>
```

| Flag | Description |
| ---- | ----------- |
| -bookieId <hostname|ip> | Bookie Id | 
| -updatespersec N | Number of ledgers updating per second (default 5 per sec) | 
| -limit N | Maximum number of ledgers to update (default no limit) | 
| -verbose | Print status of the ledger updation (default false) | 
| -printprogress N | Print messages on every configured seconds if verbose turned on (default 10 secs) | 


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

| Flag | Description |
| ---- | ----------- |
| -srcBookie BOOKIE_ID | Source Bookie Id | 
| -destBookie BOOKIE_ID | Destination Bookie Id | 
| -updatespersec N | Number of ledgers updating per second (default 5 per sec) | 
| -limit N | Maximum number of ledgers to update (default no limit) | 
| -verbose | Print status of the ledger updation (default false) | 
| -printprogress N | Print messages on every configured seconds if verbose turned on (default 10 secs) | 


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
