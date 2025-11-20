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

source "$(dirname "$0")"/common.sh

if [ ! $1 ]
then
    echo "Usage:"
    echo "   sh build.sh <flink_version> [--run-tests]"
    echo "   supported flink version: ${VERSION_MESSAGE}"
    echo "Options:"
    echo "  --run-tests        Run mvn tests (by default tests are skipped)"
    exit 1
fi

flink_minor_version=$1
check_flink_version_supported $flink_minor_version
flink_version="$(get_flink_version $flink_minor_version)"
kafka_connector_version="$(get_kafka_connector_version $flink_minor_version)"
flink_shaded_guava_version="$(get_flink_shaded_guava_version $flink_minor_version)"

# control whether to run tests (default: skip tests)
skip_tests=true
if [ "$2" = "--run-tests" ]; then
    skip_tests=false
elif [ -n "$2" ]; then
    echo "Unknown option: $2"
    echo "Use --run-tests to enable tests."
    exit 1
fi

if [ "$skip_tests" = true ]; then
  mvn_skip_flag="-DskipTests"
else
  mvn_skip_flag="-DskipTests=false"
fi

${MVN_CMD} clean package ${mvn_skip_flag} \
  -Dflink.minor.version=${flink_minor_version} \
  -Dflink.version=${flink_version} \
  -Dkafka.connector.version=${kafka_connector_version} \
  -Dflink.shaded.guava.version=${flink_shaded_guava_version}

echo "*********************************************************************"
echo "Successfully build Flink StarRocks Connector for Flink $flink_minor_version"
echo "You can find the connector jar under the \"target\" directory"
echo "*********************************************************************"

exit 0
