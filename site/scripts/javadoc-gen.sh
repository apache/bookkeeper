#!/bin/bash

source scripts/common.sh

(
  rm -rf $JAVADOC_GEN_DIR $JAVADOC_DEST_DIR
  cd $ROOT_DIR
  mvn clean install javadoc:aggregate -DskipTests
  mv $JAVADOC_GEN_DIR $JAVADOC_DEST_DIR
)
