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

import com.starrocks.data.load.stream.StreamLoadResponse;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.data.load.stream.StreamLoadConstants.RESULT_STATUS_FAILED;
import static com.starrocks.data.load.stream.StreamLoadConstants.RESULT_STATUS_INTERNAL_ERROR;

public class ErrorUtils {

    private static final List<String> UNRETRYABLE_FAIL_MESSAGE_KEY_WORDS = Arrays.asList(
                "too many filtered rows".toLowerCase(),
                "primary key size exceed the limit".toLowerCase(),
                "Only primary key table support partial update".toLowerCase(),
                "Access denied".toLowerCase(),
                "current running txns on db".toLowerCase()
            );

    public static boolean isRetryable(Throwable e) {
        if (!(e instanceof StreamLoadFailException)) {
            return true;
        }

        StreamLoadFailException exception = (StreamLoadFailException) e;
        StreamLoadResponse.StreamLoadResponseBody responseBody = exception.getResponseBody();
        if (responseBody == null) {
            return true;
        }

        if (!RESULT_STATUS_FAILED.equalsIgnoreCase(responseBody.getStatus())
            && !RESULT_STATUS_INTERNAL_ERROR.equalsIgnoreCase(responseBody.getStatus())) {
            return false;
        }

        String failMsg = responseBody.getMessage();
        if (failMsg == null) {
            return true;
        }

        failMsg = failMsg.toLowerCase();
        for (String keyword : UNRETRYABLE_FAIL_MESSAGE_KEY_WORDS) {
            if (failMsg.contains(keyword)) {
                return false;
            }
        }
        return true;
    }
}
