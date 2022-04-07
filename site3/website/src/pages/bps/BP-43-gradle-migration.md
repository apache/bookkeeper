# BP-43: Gradle migration

### Motivation
- Current maven based build system is very slow. It takes about 4m30 to execute
   mvn clean package -DskipTests while a gradle counterpart 
  `./gradlew clean jar` completes in 40 seconds on the same hardware. Using the gradle build cache.
- Incremental build- Gradle provides better incremental build support. Subsequent builds run in very little time.
- Caching of tests execution - Gradle build cache also keeps track of test successes with respect to the dependencies and can do test retries of just what has failed.
- Better handling of multi module projects - Bookkeeper is a multi module project where bookkeeper-server module for an example depends on following modules 
   - bookkeeper-stats
   - cpu-affinity
   - bookkeeper-common
   - bookkeeper-common-allocator
   - bookkeeper-http:http-server
   - bookkeeper-proto
   - circe-checksum
   - bookkeeper-tools-framework
   
  If change is made on any of the above mentioned dependent modules, building module `bookkeeper-server` will pick up those changes. 
  While in case of maven changed modules had to be built explicitly in order for the changes to be picked up.
  For instance If a change is made in `bookkeeper-common` with gradle 
  `./gradlew bookkeeper-server:test` will pick up those changes and run tests based on those changes. 
  While on the other hands for maven `mvn  install bookkeeper-common` had to run before running mvn build `bookkeeper-server`

### Public Interfaces
N/A
### Proposed Changes
- Every module and sub module which are currently being built using maven would be built using gradle.
- Run all the unit tests, functional tests etc using gradle.
- Integrating gradle build to CI/CD pipeline 
- Build ASF release capability using gradle
- Remove Maven build

### Compatibility, Deprecation, and Migration Plan
Since this is migration to the new build system using gradle, it should be done at phases.

- Phase 1: After phase one Bookkeeper should be able to build and run unit tests and integration tests using gradle. 
After the end of  this phase CI/CD pipeline for BookKeeper still remains to be the existing one. 
A sample PR may look like this.
  - Timeline: 1.5 Week
- Phase 2: Spin off new CI job for gradle which would run in parallel with existing maven based one. 
In this phase any dependency upgrade should be done at both maven as well as gradle based build system.
   - Timeline: 1 Week
   
- Phase 3: Gradle based build should be enhanced to do full CI/CD including release and a minor release should be done completely using gradle based CI/CD pipeline.
Update release docuementation as how to do release using gradle based pipeline, update information on how to setup major IDE such as IntelliJ. 
   - Timeline: 1 Week
- Phase 4: Shut down maven based CI/CD pipeline. Remove all pom.xml files
   - Timeline: Based on confidence on the new system by the community

### Test Plan

Compare test results with maven based build vs gradle based build to verify that gradle based
build is running exact same set of tests as maven based build.

### Rejected Alternatives

N/A