#/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# Steps to create the 'localhost.p12' file using OpenSSL

#Use 'apache' as password

openssl req -newkey rsa:2048 -x509 -keyout cakey.pem -out cacert.pem -days 3650
openssl pkcs12 -export -in cacert.pem -inkey cakey.pem -out localhost.p12 -name "mykey"

# Steps to create the 'client.p12' file using OpenSSL

#Use 'apache' as password
openssl req -newkey rsa:2048 -x509 -keyout cakeyclient.pem -out cacertclient.pem -days 3650
openssl pkcs12 -export -in cacertclient.pem -inkey cakeyclient.pem -out client.p12 -name "mykey"
