## BookKeeper Perf Tool

### Dlog

```shell
$ bin/bkperf dlog
Commands on evaluating performance of distributedlog library

Usage:  bkperf dlog [command] [command options]

Commands:

    read        Read log records to distributedlog streams
    write       Write log records to distributedlog streams

    help        Display help information about it
```

#### Write records to logs

```shell
$ bin/bkperf dlog write -h
Write log records to distributedlog streams

Usage:  bkperf dlog write [flags]

Flags:

    -a, --ack-quorum-size
        Ledger ack quorum size

    -e, --ensemble-size
        Ledger ensemble size

    -ln, --log-name
        Log name or log name pattern if more than 1 log is specified at
        `--num-logs`

    -b, --num-bytes
        Number of bytes to write in total. If 0, it will keep writing

    -l, --num-logs
        Number of log streams

    -n, --num-records
        Number of records to write in total. If 0, it will keep writing

    -r, --rate
        Write rate bytes/s across log streams

    -rs, --record-size
        Log record size

    --threads
        Number of threads writing

    -w, --write-quorum-size
        Ledger write quorum size


    -h, --help
        Display help information
```

Example: write to log stream `test-log` at `100mb/second`, using 1-bookie ensemble.

```shell
$ bin/bkperf dlog write -w 1 -a 1 -e 1 -r 104857600 --log-name test-log
```

### Read records from logs

```shell
$ bin/bkperf dlog read -h
Read log records from distributedlog streams

Usage:  bkperf dlog read [flags]

Flags:

    -ln, --log-name
        Log name or log name pattern if more than 1 log is specified at
        `--num-logs`

    -l, --num-logs
        Number of log streams

    --threads
        Number of threads reading


    -h, --help
        Display help information
```

Example: read from log stream `test-log-000000`.

```shell
$ bin/bkperf dlog read --log-name test-log-000000
```
