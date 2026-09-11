---
id: decomission
title: Decommission Bookies
---

In case the user wants to decommission a bookie, the following process is useful to follow in order to verify if the
decommissioning was safely done.

### Before we decommission
1. Ensure state of your cluster can support the decommissioning of the target bookie.
Check if `EnsembleSize >= Write Quorum >= Ack Quorum` stays true with one less bookie

2. Ensure target bookie shows up in the listbookies command.

3. Ensure that there is no other process ongoing (upgrade etc).

### Process of Decommissioning
1. Log on to the bookie node, check if there are underreplicated ledgers.

If there are, the decommission command will force them to be replicated.
`$ bin/bookkeeper shell listunderreplicated`

2. Stop the bookie
`$ bin/bookkeeper-daemon.sh stop bookie`

3. Run the decommission command.
If you have logged onto the node you wish to decommission, you don't need to provide `-bookieid`
If you are running the decommission command for target bookie node from another bookie node you should mention 
the target bookie id in the arguments for `-bookieid`
`$ bin/bookkeeper shell decommissionbookie`
or
`$ bin/bookkeeper shell decommissionbookie -bookieid <target bookieid>`

4. Validate that there are no ledgers on decommissioned bookie
`$ bin/bookkeeper shell listledgers -bookieid <target bookieid>`

Last step to verify is you could run this command to check if the bookie you decommissioned doesn’t show up in list bookies:

```bash
$ bin/bookkeeper shell listbookies -rw -h
$ bin/bookkeeper shell listbookies -ro -h
```
