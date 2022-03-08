#!/bin/bash

source scripts/common.sh

function build_javadoc() {
  version=$1

  echo "Building the javadoc for version ${version}."
  if [ "$version" == "latest" ]; then
    javadoc_gen_dir="${ROOT_DIR}/build/docs/javadoc"
    use_gradle=true
    cd $ROOT_DIR
  else

    rm -rf /tmp/bookkeeper-${version}
    git clone https://github.com/apache/bookkeeper  -b "release-${version}" /tmp/bookkeeper-${version}
    cd /tmp/bookkeeper-${version}
    if [[ -f "pom.xml" ]]; then
      use_gradle=false
      javadoc_gen_dir="/tmp/bookkeeper-${version}/target/site/apidocs"
    else
      use_gradle=true
      javadoc_gen_dir="/tmp/bookkeeper-${version}/build/docs/javadoc"
    fi

  fi
  javadoc_dest_dir="${ROOT_DIR}/site/docs/${version}/api/javadoc"
  rm -rf $javadoc_dest_dir
  if [[ "$use_gradle" == "true" ]]; then
    ./gradlew generateApiJavadoc
  else
    mvn clean install javadoc:aggregate -DskipTests
  fi

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
