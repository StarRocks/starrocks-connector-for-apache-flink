/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class EnvUtils {

    private static final class GitInformation {

        private static final Logger
                LOG = LoggerFactory.getLogger(EnvUtils.GitInformation.class);
        private static final String PROP_FILE = "stream-load-sdk-git.properties";
        private static final String UNKNOWN = "unknown";

        private static final GitInformation
                INSTANCE = new GitInformation();

        private String gitBuildTime = UNKNOWN;
        private String gitCommitId = UNKNOWN;
        private String gitCommitIdAbbrev = UNKNOWN;
        private String gitCommitTime = UNKNOWN;

        private String getProperty(Properties properties, String key, String defaultValue) {
            String value = properties.getProperty(key);
            if (value == null || value.charAt(0) == '$') {
                return defaultValue;
            }
            return value;
        }

        public GitInformation() {
            ClassLoader classLoader = GitInformation.class.getClassLoader();
            try (InputStream propFile = classLoader.getResourceAsStream(PROP_FILE)) {
                if (propFile != null) {
                    Properties properties = new Properties();
                    properties.load(propFile);
                    gitBuildTime = getProperty(properties, "git.build.time", UNKNOWN);
                    gitCommitId = getProperty(properties, "git.commit.id", UNKNOWN);
                    gitCommitIdAbbrev = getProperty(properties, "git.commit.id.abbrev", UNKNOWN);
                    gitCommitTime = getProperty(properties, "git.commit.time", UNKNOWN);
                }
            } catch (Exception e) {
                LOG.warn("Can't load git information, exception message: {}", e.getMessage());
            }
        }

        public String getGitBuildTime() {
            return gitBuildTime;
        }

        public String getGitCommitId() {
            return gitCommitId;
        }

        public String getGitCommitIdAbbrev() {
            return gitCommitIdAbbrev;
        }

        public String getGitCommitTime() {
            return gitCommitTime;
        }

        @Override
        public String toString() {
            return "GitInformation{" +
                    "gitBuildTime='" + gitBuildTime + '\'' +
                    ", gitCommitId='" + gitCommitId + '\'' +
                    ", gitCommitIdAbbrev='" + gitCommitIdAbbrev + '\'' +
                    ", gitCommitTime='" + gitCommitTime + '\'' +
                    '}';
        }
    }

    public static GitInformation getGitInformation() {
        return GitInformation.INSTANCE;
    }
}
