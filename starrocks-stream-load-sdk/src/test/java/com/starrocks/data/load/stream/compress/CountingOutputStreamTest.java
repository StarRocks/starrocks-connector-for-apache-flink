/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

package com.starrocks.data.load.stream.compress;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/** Tests for {@link CountingOutputStream}. */
public class CountingOutputStreamTest {

    @Test
    public void testCount() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);

        byte[] data = new byte[100];
        ThreadLocalRandom.current().nextBytes(data);
        countingOutputStream.write(data[0]);
        assertEquals(1, countingOutputStream.getCount());
        countingOutputStream.write(data, 1, data.length - 2);
        assertEquals(data.length - 1, countingOutputStream.getCount());
        countingOutputStream.write(data[data.length - 1]);
        assertEquals(data.length, countingOutputStream.getCount());
        countingOutputStream.write(data);
        assertEquals(data.length * 2, countingOutputStream.getCount());
        countingOutputStream.close();

        byte[] result = outputStream.toByteArray();
        assertEquals(data.length * 2, result.length);
        for (int i = 0; i < result.length;) {
            for (int j = 0; j < data.length; j++, i++) {
                assertEquals(data[j], result[i]);
            }
        }
        assertEquals(data.length * 2, countingOutputStream.getCount());
    }
}
