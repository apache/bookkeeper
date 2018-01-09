---
title: Third party dependencies and licensing
---

The bookkeeper project ships two binary distributions.

- ```bookkeeper-all-<version>-bin.tar.gz```, which contains the bookkeeper server and all optional dependencies.
- ```bookkeeper-server-<version>-bin.tar.gz```, which contains the bare minimum to run a bookkeeper server.

These binaries ship with third party dependencies in jar file form. As the ASF does not own the copyright on the contents of these third party jars, we need to account for them in the LICENSE file, and possibly in our NOTICE file.

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

# Handling new/updated binary dependencies

When a new dependency is added, or a dependency version is updated, we need to update the LICENSE and NOTICE files for our binary packages. There is a separate version of each of these files for both the -all tarball and the -server tarball. The files can be found at ```bookkeeper-dist/src/main/resources```.

How you update the files depends on the licensing of the dependency. Most dependencies come under either the Apache Software License, Version 2, or an MIT/BSD style license. If the software comes under anything else, it's best to ask for advice on the [dev@ list](/community/mailing-lists).

## Apache Software License, Version 2 dependencies

1. Add the jar under "The following bundled 3rd party jars are distributed under the Apache Software License, Version 2."
2. Add a link to the source code of this dependency if available. This will help anyone updating the license in the future.
3. Check the LICENSE file of the dependency.
  - If it only contains the Apache Software License, Version 2, nothing needs to be copied into our LICENSE file.
  - If there is something other than the ASLv2, then you must figure out if it refers to code that is actually shipped in the jar. If it is shipped in the jar, then check the license of that code and apply the rules to it as you would if it was a first order dependency.
4. Check the NOTICE file of the dependency, if there is one.
  - Copy any copyright notices to our NOTICE, unless they are for Apache Software Foundation (already covered by our own copyright notice), or they are refer to code covered by a BSD/MIT style license (some projects mistakenly put these in the NOTICE file, but these should be noted in the _LICENSE_ file).
  - Ensure that anything copies from the NOTICE file refers to code which is actually shipped with our tarball. Some projects put optional dependencies in their NOTICE, which are not actually pulled into our distribution, so we should not include these.

## BSD/MIT style license dependencies

1. Add a section to the LICENSE file, stating that "This product bundles X, which is available under X".
2. Add the license to ```bookkeeper-dist/src/main/resources/deps/``` and add a link to this file from the LICENSE file.
3. Ensure that the deps/ license is in the inclusion lists for the correct package assembly specs (```bookkeeper-dist/src/assemble/```).
4. The section must state the path of the jar that is covered by the license, so that the tool can pick it up.
5. Add a link to the source code of the dependency if available, to make it easier to update the dependency in future.
6. Sometimes the LICENSE of a dependency refers to a dependency which they themselves has bundled. These references should be copied to our LICENSE file, as if they were first order dependencies.

## Further resources

- [Assembling LICENSE and NOTICE](http://www.apache.org/dev/licensing-howto.html)
- [ASF Source Header and Copyright Notice Policy](http://apache.org/legal/src-headers.html)
