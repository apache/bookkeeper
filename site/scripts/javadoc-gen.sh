#!/bin/bash

source scripts/common.sh

(
  rm -rf api
  cd $ROOT_DIR
  mvn compile javadoc:aggregate
  cp -r $JAVADOC_GEN_DIR $JAVADOC_DEST_DIR
)
