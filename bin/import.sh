#!/usr/bin/env bash

BIN_PATH=$(cd `dirname $0`; pwd)

confPath=$1

if [ ! -n "$confPath" ] ;then
    echo "Usage: import.sh <schema configuration path>"
    exit 1
fi


for jarFile in `ls ${BIN_PATH}/../lib/*jar`
do
  CLASSPATH=${CLASSPATH}:${jarFile}
done

java -cp ${CLASSPATH} com.asiainfo.codis.ImportData ${confPath}

if [ $? -ne 0 ]; then
   echo "Import data failed!"
   exit 2
fi

exit 0