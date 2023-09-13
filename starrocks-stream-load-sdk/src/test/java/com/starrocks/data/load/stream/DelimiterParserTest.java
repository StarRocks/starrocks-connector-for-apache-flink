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

package com.starrocks.data.load.stream;

import org.junit.Assert;
import org.junit.Test;

public class DelimiterParserTest {

    @Test
    public void testNormal() {
        Assert.assertEquals("\n", DelimiterParser.convertDelimiter("\n"));
        Assert.assertEquals("\1", DelimiterParser.convertDelimiter("\\x01"));
        Assert.assertEquals("\0\1", DelimiterParser.convertDelimiter("\\x0001"));
        Assert.assertEquals("|", DelimiterParser.convertDelimiter("|"));
        Assert.assertEquals("\\|", DelimiterParser.convertDelimiter("\\|"));
    }

    @Test(expected = RuntimeException.class)
    public void testHexFormatError() {
        DelimiterParser.convertDelimiter("\\x0g");
    }

    @Test(expected = RuntimeException.class)
    public void testHexLengthError() {
        DelimiterParser.convertDelimiter("\\x011");
    }
}
