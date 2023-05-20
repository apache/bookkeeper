[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Detecting the operating system and CPU architecture
[INFO] ------------------------------------------------------------------------
[INFO] os.detected.name: linux
[INFO] os.detected.arch: x86_64
[INFO] os.detected.release: ubuntu
[INFO] os.detected.release.version: 22.04
[INFO] os.detected.release.like.ubuntu: true
[INFO] os.detected.release.like.debian: true
[INFO] os.detected.classifier: linux-x86_64
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Build Order:
[INFO] 
[INFO] Apache BookKeeper :: Parent                                        [pom]
[INFO] Apache BookKeeper :: Build Tools                                   [jar]
[INFO] Apache BookKeeper :: Test Tools                                    [jar]
[INFO] Apache BookKeeper :: Circe Checksum Library                        [nar]
[INFO] Apache BookKeeper :: Stats :: Parent                               [pom]
[INFO] Apache BookKeeper :: Stats API                                     [jar]
[INFO] Apache BookKeeper :: CPU Affinity Library                          [nar]
[INFO] Apache BookKeeper :: Common                                        [jar]
[INFO] Apache BookKeeper :: Common :: Allocator                           [jar]
[INFO] Apache BookKeeper :: Stats Providers                               [pom]
[INFO] Apache BookKeeper :: Stats Providers :: Codahale Metrics           [jar]
[INFO] Apache BookKeeper :: Stats Providers :: Prometheus                 [jar]
[INFO] Apache BookKeeper :: Stats :: Utils                                [jar]
[INFO] Apache BookKeeper :: Protocols                                     [jar]
[INFO] Apache BookKeeper :: Structured Logger :: Parent                   [pom]
[INFO] Apache BookKeeper :: Structured Logger :: API                      [jar]
[INFO] Apache BookKeeper :: Structured Logger :: SLF4J Implementation     [jar]
[INFO] Apache BookKeeper :: Tools :: Parent                               [pom]
[INFO] Apache BookKeeper :: Tools :: Framework                            [jar]
[INFO] Apache BookKeeper :: Native IO Library                             [nar]
[INFO] Apache BookKeeper :: Http :: Http Server                           [jar]
[INFO] Apache BookKeeper :: Bookkeeper Http :: Vertx Http Server          [jar]
[INFO] Apache BookKeeper :: Server                                        [jar]
[INFO] Apache BookKeeper :: Benchmark                                     [jar]
[INFO] Apache BookKeeper :: Bookkeeper Http :: Servlet Http Server        [jar]
[INFO] Apache BookKeeper :: Http                                          [pom]
[INFO] Apache BookKeeper :: DistributedLog :: Parent                      [pom]
[INFO] Apache BookKeeper :: DistributedLog :: Common                      [jar]
[INFO] Apache BookKeeper :: DistributedLog :: Protocol                    [jar]
[INFO] Apache BookKeeper :: DistributedLog :: Core Library                [jar]
[INFO] Apache BookKeeper :: DistributedLog :: IO :: FileSystem            [jar]
[INFO] Apache BookKeeper :: DistributedLog :: IO                          [pom]
[INFO] Apache BookKeeper :: Stream Storage :: Parent                      [pom]
[INFO] Apache BookKeeper :: Stream Storage :: Common Classes for Tests    [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Common                      [jar]
[INFO] Apache BookKeeper :: Stream Storage :: API                         [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Proto                       [jar]
[INFO] Apache BookKeeper :: Stream Storage :: State Library               [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Parent           [pom]
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: Parent [pom]
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: Base [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: KV [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client      [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Parent           [pom]
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Api              [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Impl             [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Server                      [jar]
[INFO] Apache BookKeeper :: Stream Storage :: Common :: BK Grpc Name Resolver [jar]
[INFO] Apache BookKeeper :: Tools :: Ledger                               [jar]
[INFO] Apache BookKeeper :: Tools :: Stream                               [jar]
[INFO] Apache BookKeeper :: Tools :: Perf                                 [jar]
[INFO] Apache BookKeeper :: Tools                                         [jar]
[INFO] Apache BookKeeper :: Metadata Drivers :: Parent                    [pom]
[INFO] Apache BookKeeper :: Metadata Drivers:: Etcd                       [jar]
[INFO] Apache BookKeeper :: Dist (Parent)                                 [pom]
[INFO] Apache BookKeeper :: Dist (All)                                    [jar]
[INFO] Apache BookKeeper :: Dist (Server)                                 [jar]
[INFO] Apache BookKeeper :: Dist (Bkctl)                                  [jar]
[INFO] Apache BookKeeper :: Shaded :: Parent                              [pom]
[INFO] Apache BookKeeper :: Shaded :: bookkeeper-server-shaded            [jar]
[INFO] Apache BookKeeper :: Shaded :: bookkeeper-server-tests-shaded      [jar]
[INFO] Apache BookKeeper :: Shaded :: distributedlog-core-shaded          [jar]
[INFO] Apache BookKeeper :: microbenchmarks                               [jar]
[INFO] Apache BookKeeper :: Tests                                         [pom]
[INFO] Apache BookKeeper :: Tests :: Test Shaded Jars                     [pom]
[INFO] Apache BookKeeper :: Tests :: bookkeeper-server-shaded test        [jar]
[INFO] Apache BookKeeper :: Tests :: bookkeeper-server-tests-shaded test  [jar]
[INFO] Apache BookKeeper :: Tests :: distributedlog-core-shaded test      [jar]
[INFO] Apache BookKeeper :: Tests :: Docker Images                        [pom]
[INFO] Apache BookKeeper :: Tests :: Docker Images :: All Released Versions [pom]
[INFO] Apache BookKeeper :: Tests :: Docker Images :: All Versions        [pom]
[INFO] Apache BookKeeper :: Tests :: Utility module for Arquillian based integration tests [jar]
[INFO] Apache BookKeeper :: Tests :: Common topologies for Docker based integration tests [jar]
[INFO] Apache BookKeeper :: Tests :: Base module for Arquillian based integration tests [pom]
[INFO] Apache BookKeeper :: Tests :: Base module for Arquillian based integration tests using groovy [pom]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility               [pom]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between all released versions and current version [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between 4.1.0 and current version [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test compat between old version and new version of hierarchical ledger manager [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between 4.1.0 and current version (with hostname bookie ID) [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test recovery does not work when password no in metadata [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade 4.1.0 to current in cluster with cookies [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test old clients working on current server [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between yahoo custom version and current [jar]
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test Bouncy Castle Provider load non FIPS version [jar]
[INFO] Apache BookKeeper :: Tests :: Integration                          [pom]
[INFO] Apache BookKeeper :: Tests :: Integration :: Smoke test            [jar]
[INFO] Apache BookKeeper :: Tests :: Integration :: Standalone test       [jar]
[INFO] Apache BookKeeper :: Tests :: Integration :: Cluster test          [jar]
[INFO] Apache BookKeeper :: Tests :: Bash Scripts Test                    [jar]
[INFO] 
[INFO] ------------------< org.apache.bookkeeper:bookkeeper >------------------
[INFO] Building Apache BookKeeper :: Parent 4.16.0-SNAPSHOT              [1/90]
[INFO] --------------------------------[ pom ]---------------------------------
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Apache BookKeeper :: Parent 4.16.0-SNAPSHOT:
[INFO] 
[INFO] Apache BookKeeper :: Parent ........................ FAILURE [  0.015 s]
[INFO] Apache BookKeeper :: Build Tools ................... SKIPPED
[INFO] Apache BookKeeper :: Test Tools .................... SKIPPED
[INFO] Apache BookKeeper :: Circe Checksum Library ........ SKIPPED
[INFO] Apache BookKeeper :: Stats :: Parent ............... SKIPPED
[INFO] Apache BookKeeper :: Stats API ..................... SKIPPED
[INFO] Apache BookKeeper :: CPU Affinity Library .......... SKIPPED
[INFO] Apache BookKeeper :: Common ........................ SKIPPED
[INFO] Apache BookKeeper :: Common :: Allocator ........... SKIPPED
[INFO] Apache BookKeeper :: Stats Providers ............... SKIPPED
[INFO] Apache BookKeeper :: Stats Providers :: Codahale Metrics SKIPPED
[INFO] Apache BookKeeper :: Stats Providers :: Prometheus . SKIPPED
[INFO] Apache BookKeeper :: Stats :: Utils ................ SKIPPED
[INFO] Apache BookKeeper :: Protocols ..................... SKIPPED
[INFO] Apache BookKeeper :: Structured Logger :: Parent ... SKIPPED
[INFO] Apache BookKeeper :: Structured Logger :: API ...... SKIPPED
[INFO] Apache BookKeeper :: Structured Logger :: SLF4J Implementation SKIPPED
[INFO] Apache BookKeeper :: Tools :: Parent ............... SKIPPED
[INFO] Apache BookKeeper :: Tools :: Framework ............ SKIPPED
[INFO] Apache BookKeeper :: Native IO Library ............. SKIPPED
[INFO] Apache BookKeeper :: Http :: Http Server ........... SKIPPED
[INFO] Apache BookKeeper :: Bookkeeper Http :: Vertx Http Server SKIPPED
[INFO] Apache BookKeeper :: Server ........................ SKIPPED
[INFO] Apache BookKeeper :: Benchmark ..................... SKIPPED
[INFO] Apache BookKeeper :: Bookkeeper Http :: Servlet Http Server SKIPPED
[INFO] Apache BookKeeper :: Http .......................... SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: Parent ...... SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: Common ...... SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: Protocol .... SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: Core Library  SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: IO :: FileSystem SKIPPED
[INFO] Apache BookKeeper :: DistributedLog :: IO .......... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Parent ...... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Common Classes for Tests SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Common ...... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: API ......... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Proto ....... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: State Library SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Parent SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: Parent SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: Base SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client :: KV SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Clients :: Java Client SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Parent SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Api SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Storage :: Impl SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Server ...... SKIPPED
[INFO] Apache BookKeeper :: Stream Storage :: Common :: BK Grpc Name Resolver SKIPPED
[INFO] Apache BookKeeper :: Tools :: Ledger ............... SKIPPED
[INFO] Apache BookKeeper :: Tools :: Stream ............... SKIPPED
[INFO] Apache BookKeeper :: Tools :: Perf ................. SKIPPED
[INFO] Apache BookKeeper :: Tools ......................... SKIPPED
[INFO] Apache BookKeeper :: Metadata Drivers :: Parent .... SKIPPED
[INFO] Apache BookKeeper :: Metadata Drivers:: Etcd ....... SKIPPED
[INFO] Apache BookKeeper :: Dist (Parent) ................. SKIPPED
[INFO] Apache BookKeeper :: Dist (All) .................... SKIPPED
[INFO] Apache BookKeeper :: Dist (Server) ................. SKIPPED
[INFO] Apache BookKeeper :: Dist (Bkctl) .................. SKIPPED
[INFO] Apache BookKeeper :: Shaded :: Parent .............. SKIPPED
[INFO] Apache BookKeeper :: Shaded :: bookkeeper-server-shaded SKIPPED
[INFO] Apache BookKeeper :: Shaded :: bookkeeper-server-tests-shaded SKIPPED
[INFO] Apache BookKeeper :: Shaded :: distributedlog-core-shaded SKIPPED
[INFO] Apache BookKeeper :: microbenchmarks ............... SKIPPED
[INFO] Apache BookKeeper :: Tests ......................... SKIPPED
[INFO] Apache BookKeeper :: Tests :: Test Shaded Jars ..... SKIPPED
[INFO] Apache BookKeeper :: Tests :: bookkeeper-server-shaded test SKIPPED
[INFO] Apache BookKeeper :: Tests :: bookkeeper-server-tests-shaded test SKIPPED
[INFO] Apache BookKeeper :: Tests :: distributedlog-core-shaded test SKIPPED
[INFO] Apache BookKeeper :: Tests :: Docker Images ........ SKIPPED
[INFO] Apache BookKeeper :: Tests :: Docker Images :: All Released Versions SKIPPED
[INFO] Apache BookKeeper :: Tests :: Docker Images :: All Versions SKIPPED
[INFO] Apache BookKeeper :: Tests :: Utility module for Arquillian based integration tests SKIPPED
[INFO] Apache BookKeeper :: Tests :: Common topologies for Docker based integration tests SKIPPED
[INFO] Apache BookKeeper :: Tests :: Base module for Arquillian based integration tests SKIPPED
[INFO] Apache BookKeeper :: Tests :: Base module for Arquillian based integration tests using groovy SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between all released versions and current version SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between 4.1.0 and current version SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test compat between old version and new version of hierarchical ledger manager SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between 4.1.0 and current version (with hostname bookie ID) SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test recovery does not work when password no in metadata SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade 4.1.0 to current in cluster with cookies SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test old clients working on current server SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test upgrade between yahoo custom version and current SKIPPED
[INFO] Apache BookKeeper :: Tests :: Backward Compatibility :: Test Bouncy Castle Provider load non FIPS version SKIPPED
[INFO] Apache BookKeeper :: Tests :: Integration .......... SKIPPED
[INFO] Apache BookKeeper :: Tests :: Integration :: Smoke test SKIPPED
[INFO] Apache BookKeeper :: Tests :: Integration :: Standalone test SKIPPED
[INFO] Apache BookKeeper :: Tests :: Integration :: Cluster test SKIPPED
[INFO] Apache BookKeeper :: Tests :: Bash Scripts Test .... SKIPPED
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.598 s
[INFO] Finished at: 2023-05-17T18:19:07+02:00
[INFO] ------------------------------------------------------------------------
[ERROR] Unknown lifecycle phase "bookkeeper-server". You must specify a valid lifecycle phase or a goal in the format <plugin-prefix>:<goal> or <plugin-group-id>:<plugin-artifact-id>[:<plugin-version>]:<goal>. Available lifecycle phases are: validate, initialize, generate-sources, process-sources, generate-resources, process-resources, compile, process-classes, generate-test-sources, process-test-sources, generate-test-resources, process-test-resources, test-compile, process-test-classes, test, prepare-package, package, pre-integration-test, integration-test, post-integration-test, verify, install, deploy, pre-clean, clean, post-clean, pre-site, site, post-site, site-deploy. -> [Help 1]
[ERROR] 
[ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
[ERROR] Re-run Maven using the -X switch to enable full debug logging.
[ERROR] 
[ERROR] For more information about the errors and possible solutions, please read the following articles:
[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/LifecyclePhaseNotFoundException
