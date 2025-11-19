#!/usr/bin/env bash
#
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

SUPPORTED_MINOR_VERSION=("2.0" "2.1")
SUPPORTED_KAFKA_CONNECTOR_VERSION=("4.0.1-2.0" "4.0.1-2.0")
SUPPORTED_FLINK_SHADED_VERSION=("32.1.3-jre-19.0" "33.4.0-jre-20.0")

VERSION_MESSAGE=$(IFS=, ; echo "${SUPPORTED_MINOR_VERSION[*]}")

function check_flink_version_supported() {
  local FLINK_MINOR_VERSION=$1
  if [[ " ${SUPPORTED_MINOR_VERSION[*]} " != *" $FLINK_MINOR_VERSION "* ]];
  then
      echo "Error: only support flink version: ${VERSION_MESSAGE}"
      exit 1
  fi
}

function get_flink_version() {
  local FLINK_MINOR_VERSION=$1
  echo "${FLINK_MINOR_VERSION}.0"
}

function get_kafka_connector_version() {
  local FLINK_MINOR_VERSION=$1
  local index=-1
  for ((i=0; i<${#SUPPORTED_MINOR_VERSION[@]}; i++)); do
      if [ "${SUPPORTED_MINOR_VERSION[i]}" = "$FLINK_MINOR_VERSION" ]; then
          index=$i
          break
      fi
  done

  if [ "$index" != -1 ];
  then
    local KAFKA_CONNECTOR_VERSION="${SUPPORTED_KAFKA_CONNECTOR_VERSION[index]}"
    echo $KAFKA_CONNECTOR_VERSION
  else
    echo "Can't find kafka connector version for flink-${FLINK_MINOR_VERSION}"
    exit 1
  fi
}

function get_flink_shaded_guava_version() {
  local FLINK_MINOR_VERSION=$1
  local index=-1
  for ((i=0; i<${#SUPPORTED_MINOR_VERSION[@]}; i++)); do
      if [ "${SUPPORTED_MINOR_VERSION[i]}" = "$FLINK_MINOR_VERSION" ]; then
          index=$i
          break
      fi
  done

  if [ "$index" != -1 ];
  then
    local FLINK_SHADED_VERSION="${SUPPORTED_FLINK_SHADED_VERSION[index]}"
    echo $FLINK_SHADED_VERSION
  else
    echo "Can't find flink shaded guava version for flink-${FLINK_MINOR_VERSION}"
    exit 1
  fi
}
