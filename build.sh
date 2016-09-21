#!/usr/bin/env bash

HOME_PATH=$(cd `dirname $0`; pwd)

cd ${HOME_PATH}

mvn clean package

if [ $? -ne 0 ]; then
   echo "Build failed..."
   exit 1
fi

rm -fr build

mkdir -p build/Codis_Import/logs

cp -r bin build/Codis_Import
cp -r conf build/Codis_Import

cp -r target/lib build/Codis_Import

cp target/codis-import-data-*.jar build/Codis_Import/lib

cd build

tar czf Codis_Import.tar.gz Codis_Import

exit 0
