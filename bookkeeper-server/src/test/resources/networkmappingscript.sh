#!/usr/bin/env bash
# This script is used as NetworkTopology Mapping Script in TestRackawareEnsemblePlacementPolicyUsingScript.java TestSuite
# It just maps HostAddress to rack depending on the last character of the HostAddress string
# for eg. 
#       127.0.0.1    - /1
#       127.0.0.2    - /2
#       199.12.34.21 - /1
# This script file is used just for testing purpose

for var in "$@"
do
    i=$((${#var}-1))
    echo /${var:$i:1}
done