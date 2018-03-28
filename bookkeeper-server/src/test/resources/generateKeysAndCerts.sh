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

if [ $# -eq 0 ]; then
    echo "Exiting, working directory is not provided"
    exit 1
fi

TMPDIR="${TMPDIR:-$(dirname $(mktemp))}"
CA_DIR="$TMPDIR/myCA"

# create CA directory
if [ -d "$CA_DIR" ]; then
    rm -Rf $CA_DIR
fi

#prepare CA DIR
mkdir -p $CA_DIR/newcerts
touch $CA_DIR/index.txt $CA_DIR/index.txt.attr
echo "68" > $CA_DIR/serial
echo "Using TMP dir: $CA_DIR"

#DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$1
rm $DIR/*pem $DIR/*csr $DIR/*p12 $DIR/*jks 2> /dev/null
echo "PWD: $DIR"

######## Server keys and certificates
# Create key (PEM)
# This generates RSA private key
openssl genrsa -out server-key-tmp.pem

# Strip password protection
openssl pkcs8 -topk8 -in server-key-tmp.pem -out server-key.pem -nocrypt

# Generate CSR
HOSTNAMES=`echo -e "DNS.1 = localhost\n"; echo -e "DNS.2 = $(/bin/hostname -s)\n"; idx=3; for name in $(/bin/hostname -A); do echo -e "DNS.$idx = $name\n"; idx=$((idx + 1)); done; for name in $(/bin/hostname -f); do echo -e "DNS.$idx = $name\n"; idx=$((idx + 1)); done;`

#generate all IP addresses
IP=`idx=1; for ip in $(/sbin/ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*'); do echo -e "IP.$idx = $ip\n"; idx=$((idx + 1)); done;`
str=$IP

# generate certificate request
openssl req -new -key server-key.pem -out server.csr -config <(
cat <<-EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn
x509_extensions = x509_ext

[ dn ]
C = US
ST = California
L = San Francisco
O = Bookkeeper
emailAddress = dev@bookkeeper.apache.org
CN = bookkeeper.apache.org

[ req_ext ]
keyUsage = digitalSignature, keyEncipherment
subjectAltName = @alt_names

[ alt_names ]
$HOSTNAMES
$IP

[ x509_ext ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always
extendedKeyUsage = serverAuth, clientAuth, ipsecEndSystem
basicConstraints = CA:FALSE
keyUsage = cRLSign, keyCertSign
EOF
)

# Self Sign certificate (PEM)
STARTDATE=$(date +%y%m%d%H%M00Z)
ENDDATE=$(date -v +25y +%y%m%d%H%M00Z)
if [ $? -ne 0 ]; then
    ENDDATE=$(date -d "+25 years" '+%y%m%d%H%M00Z')
fi

openssl ca -policy policy_anything -selfsign -keyfile server-key.pem -startdate $STARTDATE -enddate $ENDDATE -out server-cert.pem -in server.csr -batch -config <(
cat <<-EOF
[ ca ]
default_ca = CA_default

[ CA_default ]
dir                = $CA_DIR           # Where everything is kept
database       = $CA_DIR/index.txt # database index file.
new_certs_dir  = $CA_DIR/newcerts  # default place for new certs.
serial         = $CA_DIR/serial    # The current serial number

copy_extensions = copy
default_md = sha1          # which md to use.

[ policy_anything ]
countryName     = optional
stateOrProvinceName = optional
localityName        = optional
organizationName    = optional
organizationalUnitName  = optional
commonName      = supplied
emailAddress        = optional
EOF
) -extfile <(
cat <<-EOF
basicConstraints=CA:FALSE
subjectAltName=@alt_names

[ alt_names ]
$HOSTNAMES
$IP
EOF
)

# Convert crt/key to PKCS12 format
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem -out server-key.p12 -passout pass:server

# store password in a file
echo "server" > keyStoreServerPassword.txt

# Convert crt/key to JKS format
keytool -importkeystore -srckeystore server-key.p12 -srcstoretype pkcs12 -srcstorepass server -destkeystore server-key.jks -deststoretype jks -deststorepass server


######## Client keys and certificates
# Create key (PEM)
# This generates RSA private key
openssl genrsa -out client-key-tmp.pem

# Strip password protection
openssl pkcs8 -topk8 -in client-key-tmp.pem -out client-key.pem -nocrypt

# Generate CSR
HOSTNAMES=`echo -e "DNS.1 = localhost\n"; echo -e "DNS.2 = $(/bin/hostname -s)\n"; idx=3; for name in $(/bin/hostname -A); do echo -e "DNS.$idx = $name\n"; idx=$((idx + 1)); done;`

#generate all IP addresses
IP=`idx=1; for ip in $(/sbin/ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*'); do echo -e "IP.$idx = $ip\n"; idx=$((idx + 1)); done;`
str=$IP

#generate certificate sign request
openssl req -new -key client-key.pem -out client.csr -config <(
cat <<-EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn
x509_extensions = x509_ext

[ dn ]
C = US
ST = California
L = San Francisco
O = Bookkeeper
emailAddress = dev@bookkeeper.apache.org
CN = bookkeeper.apache.org

[ req_ext ]
keyUsage = digitalSignature, keyEncipherment
subjectAltName = @alt_names

[ alt_names ]
$HOSTNAMES
$IP

[ x509_ext ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always
extendedKeyUsage = clientAuth, clientAuth, ipsecEndSystem
basicConstraints = CA:FALSE
keyUsage = cRLSign, keyCertSign
EOF
)

# truncate index file to stop OpenSSL from complaining signing-request on same CN
cat /dev/null > $CA_DIR/index.txt

# Self Sign certificate (PEM)
STARTDATE=$(date +%y%m%d%H%M00Z)
ENDDATE=$(date -v +25y +%y%m%d%H%M00Z)
if [ $? -ne 0 ]; then
    ENDDATE=$(date -d "+25 years" '+%y%m%d%H%M00Z')
fi

openssl ca -policy policy_anything -selfsign -keyfile client-key.pem -startdate $STARTDATE -enddate $ENDDATE -out client-cert.pem -in client.csr -batch -config <(
cat <<-EOF
[ ca ]
default_ca = CA_default

[ CA_default ]
dir                = $CA_DIR           # Where everything is kept
database       = $CA_DIR/index.txt # database index file.
new_certs_dir  = $CA_DIR/newcerts  # default place for new certs.
serial         = $CA_DIR/serial    # The current serial number

copy_extensions = copy
default_md = sha1          # which md to use.

[ policy_anything ]
countryName     = optional
stateOrProvinceName = optional
localityName        = optional
organizationName    = optional
organizationalUnitName  = optional
commonName      = supplied
emailAddress        = optional
EOF
) -extfile <(
cat <<-EOF
basicConstraints=CA:FALSE
subjectAltName=@alt_names

[ alt_names ]
$HOSTNAMES
$IP
EOF
)

# Convert crt/key to PKCS12 format
openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -out client-key.p12 -passout pass:client

# store password in a file
echo "client" > keyStoreClientPassword.txt

# Convert crt/key to JKS format
keytool -importkeystore -srckeystore client-key.p12 -srcstoretype pkcs12 -srcstorepass client -destkeystore client-key.jks -deststoretype jks -deststorepass client

#create bad server certificate
cp $DIR/client-cert.pem $DIR/server-cert-bad.pem

#cleanup
rm $DIR/server-key-tmp.pem \
    $DIR/client-key-tmp.pem \
    $DIR/server.csr \
    $DIR/client.csr
