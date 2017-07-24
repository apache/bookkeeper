<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Instructions for build Jenkins bookkeeper-master job using Jenkins Job Builder


Requirements:

* Unix System
* Jenkins Job Builder (see https://docs.openstack.org/infra/jenkins-job-builder/)


The jenkins folder contains:

 - bookkeeper-master-job-configuration.yaml   (Jenkins job definition)
 - jenkins_jobs.ini   (Jenkins job builder configuration)


Build and install latest version of jenkins-job-builder;

```
$ git clone https://git.openstack.org/openstack-infra/jenkins-job-builder
```

```
$ python setup.py build
```

```
$ python setup.py install --user
```

How do I create job ?

Get an username and an "API Token" for your Jenkins configuration

In order to test jenkins job configuration, go to 'jenkins' directory and run:

```
   $ jenkins-jobs test bookkeeper-master-job-configuration.yaml
```

This will print the XML configuration of the job which will be deployed to your Jenkins

 In order to apply the configuration and create the job, run:

```
   $ jenkins-jobs --user USER --password API-TOKEN --conf jenkins_jobs.ini update bookkeeper-master-job-configuration.yaml
```

By default the job will be deployed to builds.apache.org, you can change the destination in jenkins_jobs.ini