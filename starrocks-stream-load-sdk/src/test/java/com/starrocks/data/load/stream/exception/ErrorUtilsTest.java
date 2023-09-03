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

package com.starrocks.data.load.stream.exception;

import com.starrocks.data.load.stream.StreamLoadConstants;
import com.starrocks.data.load.stream.StreamLoadResponse;
import org.junit.Test;

import static com.starrocks.data.load.stream.StreamLoadConstants.RESULT_STATUS_FAILED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ErrorUtilsTest {

    @Test
    public void testRetryable() {
        assertTrue(ErrorUtils.isRetryable(new Exception()));
        assertTrue(ErrorUtils.isRetryable(new StreamLoadFailException("unknown")));

        StreamLoadResponse.StreamLoadResponseBody body1 = new StreamLoadResponse.StreamLoadResponseBody();
        body1.setStatus(StreamLoadConstants.RESULT_STATUS_LABEL_EXISTED);
        assertFalse(ErrorUtils.isRetryable(new StreamLoadFailException("test", body1)));

        StreamLoadResponse.StreamLoadResponseBody body2 = new StreamLoadResponse.StreamLoadResponseBody();
        body2.setStatus(RESULT_STATUS_FAILED);
        body2.setMessage("memory exceed");
        assertTrue(ErrorUtils.isRetryable(new StreamLoadFailException("test", body2)));

        StreamLoadResponse.StreamLoadResponseBody body3 = new StreamLoadResponse.StreamLoadResponseBody();
        body3.setStatus(RESULT_STATUS_FAILED);
        body3.setMessage("too many filtered rows");
        assertFalse(ErrorUtils.isRetryable(new StreamLoadFailException("test", body3)));
    }
}
