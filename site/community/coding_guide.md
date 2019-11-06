---
title: Coding Guide
---

These guidelines are meant to encourage consistency and best practices among people working on _Apache BookKeeper_ code base.
They should be observed unless there is compelling reason to ignore them. We are also using checkstyle to enforce coding style.
Please refer to our [checkstyle rules](https://github.com/apache/bookkeeper/blob/master/buildtools/src/main/resources/bookkeeper/checkstyle.xml) for all enforced checkstyle rules.

### Java

Apache BookKeeper code should follow the [Sun Java Coding Convention](http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html), with the following additions.

* Lines can not be longer than 120 characters.
* Indentation should be **4 spaces**. Tabs should never be used.
* Use curly braces even for single-line ifs and elses.
* No @author tags in any javadoc.
* Use try-with-resources blocks whenever is possible.
* **TODO**s should be associated to at least one issue. E.g. `// TODO: make this parameter configurable (https://github.com/apache/bookkeeper/issues/280)`

### Dependencies

Apache BookKeeper uses following libraries a lot:

* [Guava](https://github.com/google/guava): as a fundamental core library
* [Netty](http://netty.io/): for network communications and memory buffer management.

Please use these libraries whenever possible rather than introducing more dependencies. 

Dependencies are bundled with our binary distributions, so we need to attach the relevant licenses. See [Third party dependencies and licensing](/community/licensing) for a guide on how to do this correctly.

#### Future

We prefer Java-8 Future over Guava's Listenable Future. Please use Java-8 Future whenever possible.

#### Memory

We prefer using netty _ByteBuf_ over java nio _ByteBuffer_ for internal usage. As we are using Netty Buffer for memory management.

### Logging

* Logging should be taken seriously. Please take the time to access the logs when making a change to ensure that the important things are getting logged and there is no junk there.
* Logging statements should be complete sentences with proper capitalization that are written to be read by a person not necessarily familiar with the source code.
* All loggings should be done with **SLF4j**, never _System.out_ or _System.err_.

#### Logging levels

- _INFO_ is the level you should assume the software will be run in. INFO messages are things which are not bad but which the user will definitely want to know about every time they occur.
- _TRACE_ and _DEBUG_ are both things you turn on when something is wrong and you want to figure out what is going on. _DEBUG_ should not be so fine grained that it will seriously affect performance of the program. _TRACE_ can be anything. Both _DEBUG_ and _TRACE_ statements should be considered to be wrapped in an _if (logger.isDebugEnabled)_ or _if (logger.isTraceEnabled)_ check to avoid performance degradation.
- _WARN_ and _ERROR_ indicate something that is **BAD**. Use _WARN_ if you aren't totally sure it is bad, and _ERROR_ if you are.

Please log the _stack traces_ at **ERROR** level, but never at **INFO** level or below. They can be logged at **WARN** level when they are interesting for debugging.

### Monitoring

* Apache BookKeeper uses a pluggable [StatsProvider](https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats) on exporting metrics
* Any new features should come with appropriate metrics for monitoring the feature is working correctly.
* Those metrics should be taken seriously and only export useful metrics that would be used on production on monitoring/alerting healthy of the system, or troubleshooting problems.

### Unit Tests

* New changes should come with unit tests that verify the functionality being added
* Unit tests should test the least amount of code possible. Don't start the whole server unless there is no other way to test a single class or small group of classes in isolation.
* Tests should not depend on any external resources. They need to setup and teardown their own stuff.
* It is okay to use the filesystem and network in tests since that's our business but you need to clean up them after yourself.
* _Do not_ use sleep or other timing assumptions in tests. It is always, always, wrong and will fail intermittently on any test server with other things going on that causes delays.
* We are strongly recommending adding a _timeout_ value to all our test cases, to prevent a build from completing indefinitely.
`@Test(timeout=60000)`

### Configuration

* Names should be thought through from the point of view of the person using the config.
* The default values should be thought as best value for people who runs the program without tuning parameters.
* All configuration settings should be added to [default configuration file](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf) and [documented](https://github.com/apache/bookkeeper/blob/master/site/_data/config/bk_server.yaml).

### Concurrency

Apache BookKeeper is a low latency system. So it is implemented as a purely asynchronous service. This is accomplished as follows:

* All public classes should be **thread-safe**.
* We prefer using [OrderedExecutor](https://github.com/apache/bookkeeper/blob/master/bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/OrderedExecutor.java) for executing any asynchronous actions. The mutations to same instance should be submitted to same thread to execute.
* If synchronization and locking is required, they should be in a fine granularity way.
* All threads should have proper meaningful name.
* If a class is not threadsafe, it should be annotated [@NotThreadSafe](https://github.com/misberner/jsr-305/blob/master/ri/src/main/java/javax/annotation/concurrent/NotThreadSafe.java). The instances that use this class is responsible for its synchronization.

### Backwards Compatibility
* Wire protocol should support backwards compatibility to enable no-downtime upgrades. This means the servers **MUST** be able to support requests from both old and new clients simultaneously.
* Metadata formats and data formats should support backwards compatibility.
