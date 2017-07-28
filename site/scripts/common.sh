#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
JAVADOC_GEN_DIR=${JAVADOC_GEN_DIR:-"target/site/apidocs"}
JAVADOC_DEST_DIR=${JAVADOC_DEST_DIR:-"site/javadoc"}
