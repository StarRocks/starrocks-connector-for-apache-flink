#!/usr/bin/env bash
# Modifications Copyright 2021 StarRocks Limited.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail

# check maven
MVN_CMD=mvn
if [[ ! -z ${CUSTOM_MVN} ]]; then
    MVN_CMD=${CUSTOM_MVN}
fi
if ! ${MVN_CMD} --version; then
    echo "Error: mvn is not found"
    exit 1
fi
export MVN_CMD

supported_minor_version=("1.13")
version_msg=$(IFS=, ; echo "${supported_minor_version[*]}")

supported_scala_version=("2.11" "2.12")
scala_version_msg=$(IFS=, ; echo "${supported_scala_version[*]}")
if [ ! $1 ] || [ ! $2 ]
then
    echo "Usage:"
    echo "   sh build.sh <flink_version> <scala_version>"
    echo "   supported flink version: ${version_msg}"
    echo "   supported scala version: ${scala_version_msg}"
    exit 1
fi

flink_minor_version=$1
if [[ " ${supported_minor_version[*]} " == *" $flink_minor_version "* ]];
then
    echo "Compiling connector for flink version $flink_minor_version"
else
    echo "Error: only support flink version: ${version_msg}"
    exit 1
fi

scala_version=$2
if [[ " ${supported_scala_version[*]} " == *" $scala_version "* ]];
then
    echo "Compiling connector for scala version $scala_version"
else
    echo "Error: only support scala version: ${scala_version_msg}"
    exit 1
fi

flink_version=${flink_minor_version}.0
${MVN_CMD} clean package -DskipTests -Dflink.minor.version=${flink_minor_version} -Dflink.version=${flink_version} -Dscala.version=${scala_version}

echo "*********************************************************************"
echo "Successfully build Flink StarRocks Connector for Flink $flink_minor_version"
echo "You can find the connector jar under the \"target\" directory"
echo "*********************************************************************"

exit 0
