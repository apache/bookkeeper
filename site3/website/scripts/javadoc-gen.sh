#!/bin/bash
set -e
ROOT_DIR=$(git rev-parse --show-toplevel)
function build_javadoc() {
  version=$1

  echo "Building the javadoc for version ${version}."
  if [ "$version" == "latest" ]; then
    javadoc_gen_dir="${ROOT_DIR}/target/site/apidocs"
    
    mkdir -p ${ROOT_DIR}/site3/website/build/docs/latest
    mkdir -p ${ROOT_DIR}/site3/website/build/docs/latest/api
    # keep url compatibility
    javadoc_dest_dir="${ROOT_DIR}/site3/website/build/docs/latest/api/javadoc"
    cd $ROOT_DIR
  else
    rm -rf /tmp/bookkeeper-${version}
    git clone https://github.com/apache/bookkeeper -b "release-${version}" /tmp/bookkeeper-${version}
    cd /tmp/bookkeeper-${version}
    javadoc_gen_dir="/tmp/bookkeeper-${version}/target/site/apidocs"
    # keep url compatibility
    javadoc_dest_dir="${ROOT_DIR}/site3/website/build/docs/${version}/api/javadoc"

  fi
  
  rm -rf $javadoc_dest_dir
  mvn clean -B -nsu -am -pl bookkeeper-common,bookkeeper-server,:bookkeeper-stats-api,:bookkeeper-stats-providers,:codahale-metrics-provider,:prometheus-metrics-provider install javadoc:aggregate -DskipTests -Pdelombok

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
    for d in `ls ${ROOT_DIR}/site3/website/versioned_docs`; do
      build_javadoc ${d/version-/}
    done
    ;;
  *)
    echo "Unknown version '${version}' to build doc"
    ;;
esac
