#!/bin/bash
set -e
ROOT_DIR=$(git rev-parse --show-toplevel)
WEBSITE_DIR=$ROOT_DIR/site3/website
SCRIPTS_DIR=$ROOT_DIR/site3/website/scripts

cd $WEBSITE_DIR
yarn install
OUTPUT_DIR=$WEBSITE_DIR/build

# build the website to OUTPUT_DIR
yarn build

# inject Javadocs
$SCRIPTS_DIR/javadoc-gen.sh latest