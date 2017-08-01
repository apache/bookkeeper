#!/bin/bash

set -x -e -u

# Sanity check that creates a ledger, writes a few entries, reads them and deletes the ledger.
bookkeeper shell bookiesanity
