---
title: Third party dependencies and licensing
---

The bookkeeper project ships one source distribution and two binary distributions.

- ```bookkeeper-<version>-src.tar.gz```, which contains the source code to build bookkeeper.
- ```bookkeeper-all-<version>-bin.tar.gz```, which contains the bookkeeper server and all optional dependencies.
- ```bookkeeper-server-<version>-bin.tar.gz```, which contains the bare minimum to run a bookkeeper server.

The source distribution can contain source code copied from third parties. The binaries ship with third party dependencies in jar file form. 

As the ASF may not own the copyright on the contents of this copied source code or third party jars, we may need to account for them in the LICENSE and/or NOTICE file of the distribution.

The LICENSE and NOTICE files for the source distribution are found at:
- [bookkeeper/LICENSE](https://github.com/apache/bookkeeper/blob/master/LICENSE)
- [bookkeeper/NOTICE](https://github.com/apache/bookkeeper/blob/master/NOTICE)

The LICENSE and NOTICE files for the binary distribution are found at:
- [bookkeeper/bookkeeper-dist/src/main/resources/LICENSE-all.bin.txt (for -all package)](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/LICENSE-all.bin.txt)
- [bookkeeper/bookkeeper-dist/src/main/resources/NOTICE-all.bin.txt (for -all package)](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/NOTICE-all.bin.txt)
- [bookkeeper/bookkeeper-dist/src/main/resources/LICENSE-server.bin.txt (for -server package)](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/LICENSE-server.bin.txt)
- [bookkeeper/bookkeeper-dist/src/main/resources/NOTICE-server.bin.txt (for -server package)](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/NOTICE-server.bin.txt)

When updating these files, use the following rules of thumb:
- BSD/MIT-style dependencies should be covered in LICENSE.
- Apache Software License dependences should be covered in NOTICE, but only if they themselves contain a NOTICE file.
- NOTICE should be kept to a minimum, and only contain what is legally required.
- The LICENSE and NOTICE files for the binary packages should contains everything in source LICENSE and NOTICE packages, unless the source code being referred to does not ship in the binary packages (for example, if it was copied in only for tests).
- All shipped dependencies should be mentioned in LICENSE for completeness, along with a link to their source code if available.
- Any license other than BSD/MIT-style or ASL should be discussed on [the dev list](/community/mailing-lists).
- If you have any doubts, raise them on [the dev list](/community/mailing-lists).

# Handling new/updated source dependencies

For bookkeeper, a source dependency is any code which has been copied in code form into our source tree. An example of this is [circe-checksum](https://github.com/apache/bookkeeper/tree/master/circe-checksum) which was copied into our codebase and modified. Depending on the license of source code, you may need to update the source distribution LICENSE and NOTICE files.

In the case of circe-checksum, the original code is under the Apache Software License, Version 2 (ASLv2), and there is no NOTICE file, so neither LICENSE nor NOTICE need to be updated.

If, for example, we were to copy code from [Hadoop](https://github.com/apache/hadoop), and the code in question was originally written for Hadoop, then we would not need to update LICENSE or NOTICE, as Hadoop is also licensed under the ASLv2, and while it has a NOTICE file, the part covering code originally written for Hadoop is covered by the line, "This product includes software developed by The Apache Software Foundation (http://www.apache.org/).", which already exists in our NOTICE. However, if we were to copy code from Hadoop that originally originated elsewhere, such as their [pure java CRC library](https://github.com/apache/hadoop/blob/f67237cbe7bc48a1b9088e990800b37529f1db2a/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/PureJavaCrc32C.java), this code is originally from Intel, under a BSD style license, so you would have to track down the original license, add it to [deps/](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/) and link it from our LICENSE file.

If we were to copy code from [Netty](https://github.com/netty/netty/), and the code in question was originally written for Netty, then we would need to update NOTICE with the relevant portions (i.e. the first section) from the [Netty NOTICE file](https://github.com/netty/netty/blob/4.1/NOTICE.txt), as Netty is licensed under the ASLv2 and it _does_ contain a NOTICE file. If we were to copy code from Netty which originally originated elsewhere, but had also been modified by Netty, for example [their SLF4J modifications](https://github.com/netty/netty/blob/b60e0b6a51d59fb9a98918c8783265b30531de57/common/src/main/java/io/netty/logging/CommonsLogger.java), we would need to update our NOTICE with the relevant portions (i.e. the first section) from Netty's NOTICE, and also add the SLF4J license to [deps/](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/) and link it from our LICENSE file (as it has an MIT-style license).

If we were to copy code from [Protobuf](https://github.com/google/protobuf) or [SLF4J](https://www.slf4j.org/) into our code base, then we would have to copy their license to [deps/](https://github.com/apache/bookkeeper/blob/master/bookkeeper-dist/src/main/resources/) and link it from our LICENSE file, as these projects are under [BSD-style](https://github.com/google/protobuf/blob/master/LICENSE) and [MIT-style](https://www.slf4j.org/license.html) licenses respectively.

# Handling new/updated binary dependencies

When a new binary dependency is added, or a dependency version is updated, we need to update the LICENSE and NOTICE files for our binary packages. There is a separate version of each of these files for both the -all tarball and the -server tarball. The files can be found at ```bookkeeper-dist/src/main/resources```.

How you update the files depends on the licensing of the dependency. Most dependencies come under either the Apache Software License, Version 2, or an MIT/BSD style license. If the software comes under anything else, it's best to ask for advice on the [dev@ list](/community/mailing-lists).

## dev/check-binary-license script

We provide a script which will check if the LICENSE file attached to a binary tarball matches the jar files distributed in that tarball. The goal of the script is to ensure that all shipped binaries are accounted for, and that nothing else is mentioned in the LICENSE or NOTICE files.

To check that licensing is correct, generate the tarball and run the script against it as follows (in this example I've removed references to protobuf from the LICENSE file).

```shell
~/src/bookkeeper $ mvn clean package -DskipTests
...

~/src/bookkeeper $ dev/check-binary-license bookkeeper-dist/server/target/bookkeeper-server-4.7.0-SNAPSHOT-bin.tar.gz
com.google.protobuf-protobuf-java-3.4.0.jar unaccounted for in LICENSE
deps/protobuf-3.4.0/LICENSE bundled, but not linked from LICENSE

~/src/bookkeeper $ 
```

The script checks the following:
1. If a jar file is included in the tarball, this file must be mentioned in the LICENSE file.
2. If a jar file is mentioned in LICENSE or NOTICE, then this jar file must exist in the tarball.
3. If a license exists under deps/ in the tarball, this is license must be linked in the LICENSE file.
3. If a license is linked from the LICENSE file, it must exist under deps/ in the tarball.

This script will fail the check even if only the version of the dependency has changed. This is intentional. The licensing requirements of a dependency can change between versions, so if a dependency version changes, we should check that the entries for that dependency are correct in our LICENSE and NOTICE files.

## Apache Software License, Version 2 binary dependencies

1. Add the jar under "The following bundled 3rd party jars are distributed under the Apache Software License, Version 2."
2. Add a link to the source code of this dependency if available. This will help anyone updating the license in the future.
3. Check the LICENSE file of the dependency.
  - If it only contains the Apache Software License, Version 2, nothing needs to be copied into our LICENSE file.
  - If there is something other than the ASLv2, then you must figure out if it refers to code that is actually shipped in the jar. If it is shipped in the jar, then check the license of that code and apply the rules to it as you would if it was a first order dependency.
4. Check the NOTICE file of the dependency, if there is one.
  - Copy any copyright notices to our NOTICE, unless they are for Apache Software Foundation (already covered by our own copyright notice), or they are refer to code covered by a BSD/MIT style license (some projects mistakenly put these in the NOTICE file, but these should be noted in the _LICENSE_ file).
  - Ensure that anything copies from the NOTICE file refers to code which is actually shipped with our tarball. Some projects put optional dependencies in their NOTICE, which are not actually pulled into our distribution, so we should not include these.

## BSD/MIT style license binary dependencies

1. Add a section to the LICENSE file, stating that "This product bundles X, which is available under X".
2. Add the license to ```bookkeeper-dist/src/main/resources/deps/``` and add a link to this file from the LICENSE file.
3. Ensure that the deps/ license is in the inclusion lists for the correct package assembly specs (```bookkeeper-dist/src/assemble/```).
4. The section must state the path of the jar that is covered by the license, so that the tool can pick it up.
5. Add a link to the source code of the dependency if available, to make it easier to update the dependency in future.
6. Sometimes the LICENSE of a dependency refers to a dependency which they themselves has bundled. These references should be copied to our LICENSE file, as if they were first order dependencies.

## Further resources

- [Assembling LICENSE and NOTICE](http://www.apache.org/dev/licensing-howto.html)
- [ASF Source Header and Copyright Notice Policy](http://apache.org/legal/src-headers.html)
