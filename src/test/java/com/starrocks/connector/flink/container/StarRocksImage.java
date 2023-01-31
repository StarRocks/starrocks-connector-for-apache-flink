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

/** Information of a StarRocks image.  */
public class StarRocksImage {

    private final String imageName;
    private final String starRocksHome;
    private final String starRocksVersion;

    public StarRocksImage(String imageName, String starRocksHome, String starRocksVersion) {
        this.imageName = imageName;
        this.starRocksHome = starRocksHome;
        this.starRocksVersion = starRocksVersion;
    }

    public String getImageName() {
        return imageName;
    }

    public String getStarRocksHome() {
        return starRocksHome;
    }

    public String getStarRocksVersion() {
        return starRocksVersion;
    }

    @Override
    public String toString() {
        return "StarRocksImage{" +
                "imageName='" + imageName + '\'' +
                ", starRocksHome='" + starRocksHome + '\'' +
                ", starRocksVersion='" + starRocksVersion + '\'' +
                '}';
    }
}
