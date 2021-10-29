---
title: Journal/Ledger directories expand or shrink
---

If you want to expand or shrink journal/ledger directories, you can follow the following operation guide.

**STEP1**. Get bookie instanceId 

```bash
$bin/bookkeeper shell whatisinstanceid
``` 
You will get an unique instanceId, For example:
```
88e02567-a559-4145-bc02-eb33b51c180f
```

**STEP2**. Generate the latest version file according to the `instanceId`. In this command, you should configure the updated journal and ledger directory configuration, no matter shrink or expand directories.

```bash   
$bin/bookkeeper shell cookie_generate -i <instanceid> --journal-dirs /data1/xxx,/data2/xxx... --ledger-dirs /data3/yyy,/data4/yyy... -o VERSION ip:port
```

For example:

```bash
$bin/bookkeeper shell cookie_generate -i 88e02567-a559-4145-bc02-eb33b51c180f --journal-dirs /data1/pulsar-bookie/tmp/bk-journal,/data2/pulsar-bookie/tmp/bk-journal,/data3/pulsar-bookie/tmp/bk-journal --ledger-dirs /data5/pulsar-bookie/ledgers,/data6/pulsar-bookie/ledgers -o VERSION 127.0.0.1:3181
```

After execte the above command, you will generate a `VERSION` file. The content of the `VERSION` file is:

```
4
bookieHost: "127.0.0.1:3181"
journalDir: "/data1/pulsar-bookie/tmp/bk-journal,/data2/pulsar-bookie/tmp/bk-journal,/data3/pulsar-bookie/tmp/bk-journal"
ledgerDirs: "2\t/data5/pulsar-bookie/ledgers\t/data6/pulsar-bookie/ledgers"
instanceId: "88e02567-a559-4145-bc02-eb33b51c180f"
```

**STEP3**. Copy the generated `VERSION` file to all journal and ledger's current directory

```bash
   $cp VERSION /data1/xxx/current/
```
   
For example:

```bash
cp VERSION /data1/pulsar-bookie/tmp/bk-journal/current/
cp VERSION /data2/pulsar-bookie/tmp/bk-journal/current/
cp VERSION /data3/pulsar-bookie/tmp/bk-journal/current/
cp VERSION /data5/pulsar-bookie/ledgers/current/
cp VERSION /data6/pulsar-bookie/ledgers/current/
```

**STEP4**. Use the generated `VERSION` file to Update cookie to Zookeeper

```bash
$bin/bookkeeper shell cookie_update --cookie-file ./VERSION ip:port
```

For example：

```bash
bin/bookkeeper shell cookie_update --cookie-file ./VERSION 127.0.0.1:3181
```

**STEP5**. update `conf/bookkeeper.conf` file, keep `journalDirectories/ledgerDirectories` parameters configured value sync with **STEP2** configured value

**STEP6**. restart bookie

```bash
$bin/pulsar-daemon stop bookie 
$bin/pulsar-daemon start bookie 
```
