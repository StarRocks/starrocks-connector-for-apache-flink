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

package com.starrocks.connector.flink.container;

import com.github.dockerjava.api.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Builds a StarRocks image. */
public class StarRocksImageBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksImageBuilder.class);

    private static final String DEFAULT_IMAGE_NAME = "starrocks-for-flink-connector";

    /** Default base image. */
    public static final String BASE_IMAGE_NAME = "centos:centos7";

    /** Base url to download StarRocks binary. */
    public static final String STARROCKS_BINARY_BASE_URL =
            "https://releases.starrocks.io/starrocks/StarRocks-%s.tar.gz";

    /** Default version of StarRocks used to build the image. */
    public static final String DEFAULT_STARROCKS_VERSION = "2.3.7";

    /** Default home path of StarRocks in the image. */
    public static final String DEFAULT_WORKING_DIR = "/data";

    /** Basic packages needed to install. */
    public static final List<String> DEFAULT_PACKAGES_TO_INSTALL =
            Arrays.asList("which", "wget", "net-tools",  "java-1.8.0-openjdk-devel.x86_64", "mysql");

    /**
     * Default timeout to build the image. The binary size for StarRocks 2.3.0 is about 1.5 GB, and
     * it takes about 14 min to download with the network speed 1.9 MB/s in my local environment.
     * So the default timeout is set larger.
     */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(20);

    private String baseImageName = BASE_IMAGE_NAME;
    private String starRocksVersion = DEFAULT_STARROCKS_VERSION;
    private String workingDir = DEFAULT_WORKING_DIR;
    private final List<String> packagesToInstall = new ArrayList<>(DEFAULT_PACKAGES_TO_INSTALL);

    private String imageName;

    private Duration timeout = DEFAULT_TIMEOUT;

    /** Whether to delete the image when JVM exits. */
    private boolean deleteOnExit = true;

    public StarRocksImageBuilder() {
    }

    public StarRocksImageBuilder setBaseImageName(String imageName) {
        this.baseImageName = imageName;
        return this;
    }

    public StarRocksImageBuilder setStarRocksVersion(String version) {
        this.starRocksVersion = version;
        return this;
    }

    public StarRocksImageBuilder setWorkingDir(String workingDir) {
        this.workingDir = workingDir;
        return this;
    }

    public StarRocksImageBuilder addPackageToInstall(String packageName) {
        this.packagesToInstall.add(packageName);
        return this;
    }

    public StarRocksImageBuilder setImageName(String imageName) {
        this.imageName = imageName;
        return this;
    }

    public StarRocksImageBuilder setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public StarRocksImageBuilder setDeleteOnExit(boolean deleteOnExit) {
        this.deleteOnExit = deleteOnExit;
        return this;
    }

    public StarRocksImage build() {
        String imageFullName = getImageFullName();
        String starRocksHome = getStarRocksHome();

        if (checkImageExists(imageFullName)) {
            LOG.info("StarRocks image {} already exists, and skip to build it.", imageFullName);
            return new StarRocksImage(imageFullName, starRocksHome, starRocksVersion);
        }

        String installedPackages = String.join(" ", packagesToInstall);
        String starRocksBinaryUrl = getStarRocksBinaryUrl();
        LOG.info("Building StarRocks image {} with starRocksHome: {}, installedPackages: {}, and binary url: {}.",
                imageFullName, starRocksHome, installedPackages, starRocksBinaryUrl);

        try {
            new ImageFromDockerfile(imageFullName, deleteOnExit)
                    .withDockerfileFromBuilder(builder ->
                            builder
                                    .from(baseImageName)
                                    .run("yum -y install " + installedPackages)
                                    .run("mkdir -p " + starRocksHome)
                                    .run(String.format("wget -SO %s %s",
                                            new File(workingDir, "starrocks.tar.gz").getAbsolutePath(),
                                            starRocksBinaryUrl))
                                    .run(String.format(
                                            "cd %s && tar zxf starrocks.tar.gz -C %s --strip-components 1 && rm starrocks.tar.gz",
                                            workingDir, starRocksHome))
                                    .run(String.format("cd %s && mkdir -p %s && mkdir -p %s",
                                            starRocksHome, "fe/meta", "be/storage")))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            String errMsg = "Failed to build image " + imageFullName;
            LOG.error("{}", errMsg, e);
            throw new StarRocksContainerException(errMsg, e);
        }

        LOG.info("Build StarRocks image {} successfully.", imageFullName);
        return new StarRocksImage(imageFullName, starRocksHome, starRocksVersion);
    }

    private String getStarRocksBinaryUrl() {
        return String.format(STARROCKS_BINARY_BASE_URL, starRocksVersion);
    }

    private String getStarRocksHome() {
        return new File(workingDir, "starrocks").getAbsolutePath();
    }

    private boolean checkImageExists(String imageName) {
        try {
            DockerClientFactory.instance().client().inspectImageCmd(imageName).exec();
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    private String getImageFullName() {
        if (imageName != null) {
            return imageName;
        }

        return String.join(":", DEFAULT_IMAGE_NAME, starRocksVersion);
    }

    public static void main(String[] args) {
        StarRocksImage starRocksImage = new StarRocksImageBuilder()
                                            .setDeleteOnExit(false)
                                            .build();
        LOG.info("{}", starRocksImage);
    }
}
