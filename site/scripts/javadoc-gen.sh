#!/bin/bash

source scripts/common.sh

function build_javadoc() {
  version=$1

  echo "Building the javadoc for version ${version}."
  if [ "$version" == "latest" ]; then
    javadoc_gen_dir="${ROOT_DIR}/target/site/apidocs"
    cd $ROOT_DIR
  else
    javadoc_gen_dir="/tmp/bookkeeper-${version}/target/site/apidocs"
    rm -rf /tmp/bookkeeper-${version}
    git clone https://github.com/apache/bookkeeper /tmp/bookkeeper-${version}
    cd /tmp/bookkeeper-${version}
    git checkout "release-${version}"
  fi
  javadoc_dest_dir="${ROOT_DIR}/site/docs/${version}/api/javadoc"
  rm -rf $javadoc_dest_dir
  mvn clean install javadoc:aggregate -DskipTests
  mv $javadoc_gen_dir $javadoc_dest_dir

  echo "Built the javadoc for version ${version}."
}

# get arguments
version=$1
shift

case "${version}" in
  latest)
    build_javadoc ${version}
    ;;
  all)
    for d in `ls ${ROOT_DIR}/site/docs`; do
      build_javadoc $d
    done
    ;;
  *)
    echo "Unknown version '${version}' to build doc"
    ;;
esac
