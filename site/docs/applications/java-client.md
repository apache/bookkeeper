---
title: The BookKeeper Java client
javadoc_button: true
---

To get started with the Java client for BookKeeper

## Installation

The BookKeeper Java client library is available via [Maven Central](http://search.maven.org/) and can be installed using [Maven](#maven), [Gradle](#gradle), and other build tools.

### Maven

If you're using [Maven](https://maven.apache.org/) add this to your [`pom.xml`](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html) build configuration file:

```xml
<!-- in your <properties> block -->
<bookkeeper.version>{{ site.bk_version }}</bookkeeper.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.bookkeeper</groupId>
  <artifactId>bookkeeper-server</artifactId>
  <version>${bookkeeper.version}</version>
</dependency>
```

### Gradle

If you're using [Gradle](https://gradle.org/), add this to your [`build.gradle`](https://spring.io/guides/gs/gradle/) build configuration file:

```groovy
dependencies {
    compile group: 'org.apache.bookkeeper', name: 'bookkeeper', version: '{{ site.bk_version }}'
}

// Alternatively:
dependencies {
    compile 'org.apache.bookkeeper:bookkeeper-server:{{ site.bk_version }}'
}
```
