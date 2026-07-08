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

SUPPORTED_MINOR_VERSION=("1.16" "1.17" "1.18" "1.19" "1.20" "2.0" "2.1" "2.2" "2.3")
# version formats are different among flink versions; kafka connector is
# test-only, 2.1/2.3 have no dedicated release yet so the nearest is used
SUPPORTED_KAFKA_CONNECTOR_VERSION=("1.16.0" "1.17.0" "3.0.1-1.18" "3.2.0-1.19" "3.4.0-1.20" "4.0.1-2.0" "4.0.1-2.0" "5.0.0-2.2" "5.0.0-2.2")
# minimum JDK per flink version (Flink ships Java 17 class files since 2.2)
REQUIRED_JDK_VERSION=("8" "8" "8" "8" "8" "11" "11" "17" "17")
VERSION_MESSAGE=$(IFS=, ; echo "${SUPPORTED_MINOR_VERSION[*]}")

function print_supported_minor_versions() {
  echo "${SUPPORTED_MINOR_VERSION[*]}"
}

# Allow tooling to query metadata without requiring maven, e.g.:
#   bash common.sh supported-minor-versions
# (kept above the maven check so it stays usable on machines without maven)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    case "${1:-}" in
        supported-minor-versions) print_supported_minor_versions; exit 0 ;;
    esac
fi

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

function get_module_for_version() {
  local FLINK_MINOR_VERSION=$1
  case "$FLINK_MINOR_VERSION" in
    1.*) echo "flink-connector-starrocks-1.x" ;;
    2.*) echo "flink-connector-starrocks-2.x" ;;
    *)
      echo "Error: cannot map flink version $FLINK_MINOR_VERSION to a module" >&2
      exit 1
      ;;
  esac
}

# fail fast when the JDK is too old for the target flink version
function check_jdk_for_version() {
  local FLINK_MINOR_VERSION=$1
  local REQUIRED=8
  for ((i=0; i<${#SUPPORTED_MINOR_VERSION[@]}; i++)); do
      if [ "${SUPPORTED_MINOR_VERSION[i]}" = "$FLINK_MINOR_VERSION" ]; then
          REQUIRED="${REQUIRED_JDK_VERSION[i]}"
          break
      fi
  done
  local JAVA_BIN="java"
  if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_BIN="${JAVA_HOME}/bin/java"
  fi
  local RAW_VERSION
  RAW_VERSION=$("${JAVA_BIN}" -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')
  local MAJOR=${RAW_VERSION%%.*}
  if [[ "$MAJOR" == "1" ]]; then
    MAJOR=$(echo "$RAW_VERSION" | cut -d. -f2)
  fi
  if [[ -z "$MAJOR" || "$MAJOR" -lt "$REQUIRED" ]]; then
    echo "Error: building for Flink ${FLINK_MINOR_VERSION} requires JDK ${REQUIRED}+, found java version ${RAW_VERSION:-unknown}"
    exit 1
  fi
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
