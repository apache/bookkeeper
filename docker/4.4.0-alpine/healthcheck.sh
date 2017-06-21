#!/bin/bash

set -x -e -u

# Simple healtcheck on BK port
nc -z localhost ${BK_PORT}
