<img src="https://pbs.twimg.com/profile_images/545716709311520769/piLLa1iC_400x400.png" alt="logo" style="width: 32px;"/>

[![Build Status](https://travis-ci.org/apache/bookkeeper.svg?branch=master)](https://travis-ci.org/apache/bookkeeper)
[![Build Status](https://builds.apache.org/buildStatus/icon?job=bookkeeper-master)](https://builds.apache.org/job/bookkeeper-master/)
[![Coverage Status](https://coveralls.io/repos/github/apache/bookkeeper/badge.svg?branch=master)](https://coveralls.io/github/apache/bookkeeper?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.bookkeeper/bookkeeper/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.bookkeeper/bookkeeper)

# Apache BookKeeper

Apache BookKeeper is a scalable, fault tolerant and low latency storage service optimized for append-only workloads.

It is suitable for being used in following scenarios:

- WAL (Write-Ahead-Logging), e.g. HDFS NameNode.
- Message Store, e.g. Apache Pulsar.
- Offset/Cursor Store, e.g. Apache Pulsar.
- Object/Blob Store, e.g. storing state machine snapshots.

## Get Started

* *Concepts*: Start with the [basic concepts](https://bookkeeper.apache.org/docs/master/bookkeeperOverview.html) of Apache BookKeeper.
  This will help you to fully understand the other parts of the documentation.
* [Getting Started](https://bookkeeper.apache.org/docs/master/bookkeeperStarted.html) to setup BookKeeper to write logs.

## Documentation

### Developers

* [Programmer Guide](https://bookkeeper.apache.org/archives/docs/master/bookkeeperProgrammer.html)
* [Tutorial](https://bookkeeper.apache.org/archives/docs/master/bookkeeperTutorial.html)
* [Java API](https://bookkeeper.apache.org/archives/docs/master/apidocs/)

You can also read [Turning Ledgers into Logs](https://bookkeeper.apache.org/docs/master/bookkeeperLedgers2Logs.html) to learn how to turn **ledgers** into continuous **log streams**.
If you are looking for a high level **log stream** API, you can checkout [DistributedLog](http://distributedlog.io).

### Administrators

* [Admin Guide](https://bookkeeper.apache.org/docs/master/bookkeeperConfig.html)
* [Configuration Parameters](https://bookkeeper.apache.org/docs/master/bookieConfigParams.html)

### Contributors

* [BookKeeper Internals](https://bookkeeper.apache.org/docs/master/bookkeeperInternals.html)

## Get In Touch

### Report a Bug

For filing bugs, suggesting improvements, or requesting new features, help us out by [opening a Github issue](https://github.com/apache/bookkeeper/issues) or [opening an Apache jira](https://issues.apache.org/jira/browse/BOOKKEEPER).

### Need Help?

[Subscribe](mailto:user-subscribe@bookkeeper.apache.org) or [mail](mailto:user@bookkeeper.apache.org) the [user@bookkeeper.apache.org](mailto:user@bookkeeper.apache.org) list - Ask questions, find answers, and also help other users.

[Subscribe](mailto:dev-subscribe@bookkeeper.apache.org) or [mail](mailto:dev@bookkeeper.apache.org) the [dev@bookkeeper.apache.org](mailto:dev@bookkeeper.apache.org) list - Join development discussions, propose new ideas and connect with contributors.

[Join us on Slack](https://apachebookkeeper.herokuapp.com/) - This is the most immediate way to connect with Apache BookKeeper committers and contributors.

## Contributing

We feel that a welcoming open community is important and welcome contributions.

### Contributing Code

1. See [Developer Setup](https://cwiki.apache.org/confluence/display/BOOKKEEPER/Developer+Setup) to get your local environment setup.

2. Take a look at our open issues: [JIRA Issues](https://issues.apache.org/jira/browse/BOOKKEEPER) [Github Issues](https://github.com/apache/bookkeeper/issues).

3. Review our [coding style](https://cwiki.apache.org/confluence/display/BOOKKEEPER/Coding+Guide) and follow our [pull requests](https://github.com/apache/bookkeeper/pulls) to learn about our conventions.

4. Make your changes according to our [contribution guide](https://cwiki.apache.org/confluence/display/BOOKKEEPER/Contributing+to+BookKeeper).

### Improving Website and Documentation

1. See [Building the website and documentation](https://cwiki.apache.org/confluence/display/BOOKKEEPER/Building+the+website+and+documentation) on how to build the website and documentation.

