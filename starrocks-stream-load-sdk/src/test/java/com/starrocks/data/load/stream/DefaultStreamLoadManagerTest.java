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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link DefaultStreamLoadManager}. */
public class DefaultStreamLoadManagerTest {

    @Test
    public void testUseBatchTableRegion() {
        StarRocksVersion version_19 = StarRocksVersion.parse("1.9.0");
        StarRocksVersion version_22 = StarRocksVersion.parse("2.2.4");
        StarRocksVersion version_24 = StarRocksVersion.parse("2.4.1");

        // verify csv format
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, false, version_19));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, false, version_22));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, false, version_24));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, false, null));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, true, version_19));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, true, version_22));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, true, version_24));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.CSV, true, null));

        // verify json format
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, false, version_19));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, false, version_22));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, false, version_24));
        assertFalse(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, false, null));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, version_19));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, version_22));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, version_24));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, version_19));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, version_22));
        assertTrue(DefaultStreamLoadManager.useBatchTableRegion(
                StreamLoadDataFormat.JSON, true, null));

    }
}
