import common_job_properties

// This is the Java precommit which runs a maven install, and the current set of precommit tests.
mavenJob('bookkeeper_precommit_pullrequest_java9_testdf') {
  description('precommit verification for pull requests of <a href="http://bookkeeper.apache.org">Apache BookKeeper</a> in Java 9.')

  // Temporary information gathering to see if full disks are causing the builds to flake
  preBuildSteps {
    shell("df -h")
  }

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    'JDK 1.9 (latest)',
    200)

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Maven build project
  goals('clean')
}
