#!/bin/zsh

PWD=$(pwd)
MODULE=$(basename $PWD)

BUBING_VERSION=$(grep "^version=" ${PWD}/build.properties | cut -d '=' -f 2)

[[ -e ${PWD}/bubing-${BUBING_VERSION}.jar ]] || {
        echo "ERROR: can't find ${PWD}/bubing-${BUBING_VERSION}.jar"
        exit 1
}

[[ -d target ]] || {
  echo "ERROR: can't find target dir, compile (ant jar) before package"
  exit 1
}

OUTPUT_DIR=target/${MODULE}

[[ -d ${OUTPUT_DIR} ]] && rm -rf ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}
cp -r scripts/* ${OUTPUT_DIR}/
chmod u+x ${OUTPUT_DIR}/*.sh
mkdir ${OUTPUT_DIR}/jars
cp ${PWD}/bubing-${BUBING_VERSION}.jar ${OUTPUT_DIR}/jars
pushd ${OUTPUT_DIR}
ln -s ${PWD}/jars/runtime dependencies
ln -s ${PWD}/extjars extjars
popd

tar zchf ${MODULE}.tar.gz ${OUTPUT_DIR}
scp ${MODULE}.tar.gz exensa@saltmaster.exensa.net:/srv/salt/crawling/files/dev


