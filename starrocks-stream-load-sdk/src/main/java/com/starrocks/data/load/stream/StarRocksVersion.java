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

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Representation of starrocks version. */
public class StarRocksVersion implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksVersion.class);

    // The pattern of the official starrocks release should be link <major>.<minor>.<patch> such as 2.3.4
    private static final Pattern PATTERN = Pattern.compile("^(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<patch>\\d+).*");

    private int major;
    private int minor;
    private int patch;

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    @Override
    public String toString() {
        return "StarRocksVersion{" +
                "major=" + major +
                ", minor=" + minor +
                ", patch=" + patch +
                '}';
    }

    /** Parse the version string, and return null if it's not a valid version pattern */
    public static StarRocksVersion parse(String versionStr) {
        if (versionStr == null) {
            return null;
        }

        try {
            Matcher match = PATTERN.matcher(versionStr.trim());
            if (match.matches()) {
                StarRocksVersion version = new StarRocksVersion();
                version.major = Integer.parseInt(match.group("major"));
                version.minor = Integer.parseInt(match.group("minor"));
                version.patch = Integer.parseInt(match.group("patch"));
                LOG.info("Successful to parse starrocks version {}, {}", versionStr, version);
                return version;
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse starrocks version {}", versionStr, e);
        }
        LOG.info("Fail to parse starrocks version {}", versionStr);
        return null;
    }
}
