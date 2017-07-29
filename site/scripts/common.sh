#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
REVISION=$(git rev-parse --short HEAD)
JAVADOC_GEN_DIR=${JAVADOC_GEN_DIR:-"target/site/apidocs"}
JAVADOC_DEST_DIR=${JAVADOC_DEST_DIR:-"site/javadoc"}
LOCAL_GENERATED_DIR=$ROOT_DIR/site/local-generated
APACHE_GENERATED_DIR=$ROOT_DIR/site/generated_site
TMP_DIR=/tmp/bookkeeper-site
