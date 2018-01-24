#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# This script generates keys and self-signed certificates in
# PEM, JKS and PKCS12 formats.

# remove existing files.
rm ./server-key.pem \
    ./server-key.p12 \
    ./server-key.jks \
    ./server-cert.pem \
    ./client-key.pem \
    ./client-key.p12 \
    ./client-key.jks \
    ./client-cert.pem \
    ./keyStoreServerPassword.txt \
    ./keyStoreClientPassword.txt


# One line command to create server keys and self signed certificates.
openssl req \
        -new \
        -newkey rsa:4096 \
        -days 365000 \
        -nodes \
        -x509 \
        -subj "/C=US/ST=CA/L=San Francisco/O=Dummy/CN=apache.bookkeeper.org" \
        -out server-cert.pem \
        -keyout server-key.pem

# Convert crt/key to PKCS12 format.
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem -out server-key.p12 -passout pass:server

# store password in a file
echo "server" > keyStoreServerPassword.txt

# Convert crt/key to JKS format.
keytool -importkeystore -srckeystore server-key.p12 -srcstoretype pkcs12 -srcstorepass server -destkeystore server-key.jks -deststoretype jks -deststorepass server

# One line command to create client keys and self signed certificates.
openssl req \
        -new \
        -newkey rsa:4096 \
        -days 365000 \
        -nodes \
        -x509 \
        -subj "/C=US/ST=CA/L=San Francisco/O=Dummy/CN=apache.bookkeeper.org" \
        -out client-cert.pem \
        -keyout client-key.pem

# Convert crt/key to PKCS12 format.
openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -out client-key.p12 -passout pass:client

# store password in a file.
echo "client" > keyStoreClientPassword.txt

# Convert crt/key to JKS format.
keytool -importkeystore -srckeystore client-key.p12 -srcstoretype pkcs12 -srcstorepass client -destkeystore client-key.jks -deststoretype jks -deststorepass client

