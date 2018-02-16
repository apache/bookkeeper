---
title: "BP-27: New BookKeeper CLI"
issue: https://github.com/apache/bookkeeper/1000
state: 'Under Discussion'
release: "N/A"
---

### Motivation

`BookieShell` is the current bookkeeper cli for interacting and operating a bookkeeper cluster. However, this class is getting bigger with more commands added to it. It is facing a few problems for maintenance and extensibility.

- All commands sit in one gaint shell class. It is hard to tell if a command is used for managing a bookie only or if a command is used for managing a cluster.
- Lack of unit tests. This class has very few test coverage. Most of the commands (introduced in early days) don't have a unit test.
- Lack of extensibility. If a new function component (for example, dlog) is introduced, it is a bit hard to extend this CLI to have commands for new function component.

All these problems lead to the proposal here. This proposal is to propose refactoring/redesigning the bookkeeper CLI to allow better managebility for maintenance, better test coverage and better extensibility for new function components.

### Public Interfaces

This proposal will not change existing `BookieShell`. All functionalities will remain as same when using `bin/bookkeeper shell`.
Instead a new module `bookkeeper-tools` will be introduced for developing the new BookKeeper CLI and a new script `bin/bookkeeper-cli` for executing CLI commands.

The new bookkeeper CLI follows the pattern that pulsar-admin is using. The CLI commandline format would be:

```
$ bookkeeper-cli [general options] <command-group> <command> [options of command]
```

#### CommandGroup and Command

`<command-group>` and `<command>` are introduced for categorizing the commands into groups. So the commands within same group have same operation scope (e.g. whether a command is applied to a cluster or a command is applied to a bookie).

When a new function component is introduced, all its related commands can be managed in its own command group and register the group to CLI. This would allow flexible extensibility and make maintenance easier and clearer.

The proposed command groups are:

- "cluster": commands that operate on a cluster
- "bookie": commands that operate on a single bookie
- "metadata": commands that operate with metadata store
- "client": commands that use a bookkeeper client for interacting with a cluster.

Example Outputs for the new BookKeeper CLI:

- Show all command groups: `bookkeeper-cli --help`

```
Usage: bookkeeper-cli [options] [command] [command options]
  Options:
    -c, --conf
       Bookie Configuration File
    -h, --help
       Show this help message
       Default: false
  Commands:
    bookie      Commands on operating a single bookie
      Usage: bookie [options]

    client      Commands that interact with a cluster
      Usage: client [options]

    cluster      Commands that operate a cluster
      Usage: cluster [options]

    metadata      Commands that interact with metadata storage
      Usage: metadata [options]
```
- Show commands under `cluster`: `bookkeeper-cli cluster --help`
```
Usage: bookie-shell cluster [options] [command] [command options]
  Commands:
    listbookies      List the bookies, which are running as either readwrite or readonly mode.
      Usage: listbookies [options]
        Options:
          -ro, --readonly
             Print readonly bookies
             Default: false
          -rw, --readwrite
             Print readwrite bookies
             Default: false
```

### Proposed Changes

- Introduced a new module called `bookkeeper-tools` for developing the new CLI.
- The new CLI will use [JCommander](http://jcommander.org) for parse command line paramters: better on supporting this proposal commandline syntax.
- All the actual logic of the commands will be organized under `org.apache.bookkeeper.tools.cli.commands`. Each command group has its own subpackage and each command will be a class file under that command-group subpackage.
  Doing this provides better testability, since the command logic is limited in one file rather than in a gaint shell class. Proposed layout can be found [here](https://github.com/sijie/bookkeeper/tree/bookie_shell_refactor/bookkeeper-server/src/main/java/org/apache/bookkeeper/tools/cli/commands).
- For each command: the logic of a command will be moved out of `BookieShell` to its own class `org.apache.bookkeeper.tools.cli.commands.<command-group>.<CommandClass>.java`. The old BookieShell will use the new Command class and delegate the actual logic.

An initial prototype is available: https://github.com/sijie/bookkeeper/tree/bookie_shell_refactor

### Compatibility, Deprecation, and Migration Plan

`bin/bookkeeper shell` and `bin/bookkeeper-cli` will co-exist for a few releases. After `bin/bookkeeper-cli` takes over all the functionalities of `BookieShell`, we will consider deprecating the old `BookieShell`.

So no compatibility concern at this moment.

### Test Plan

When a command is moved from BookieShell to `org.apache.bookkeeper.tools.cli`, several unit tests should be added:

- Unit tests for the command logic itself.
- Unit tests in new CLI for this command.
- Unit tests in old BookieShell for this command.

### Rejected Alternatives

Another proposal is to redesign the bookkeeper CLI using admin REST api. This is not considered at this moment because some of the commands are not well supported in admin REST api (for example, metaformat, bookieformat, and most of the commands
used for troubleshooting individual bookies). If we want to support a CLI using admin REST api, we can have a separate CLI called `bookkeeper-rest-ci` to use admin REST api for operating the cluster.
