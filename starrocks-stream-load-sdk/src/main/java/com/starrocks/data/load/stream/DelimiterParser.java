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

import java.io.StringWriter;

/**
 * Parse the delimiter which contains escape characters. The logic is same as
 * that in StarRocks, See class Delimiter of StarRocks FE.
 */
public class DelimiterParser {

    private static final String HEX_STRING = "0123456789ABCDEF";

    private static byte[] hexStrToBytes(String hexStr) {
        String upperHexStr = hexStr.toUpperCase();
        int length = upperHexStr.length() / 2;
        char[] hexChars = upperHexStr.toCharArray();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | (0xff & charToByte(hexChars[pos + 1])));
        }
        return bytes;
    }

    private static byte charToByte(char c) {
        return (byte) HEX_STRING.indexOf(c);
    }

    public static String convertDelimiter(String originStr) {
        if (originStr == null || originStr.isEmpty()) {
            throw new RuntimeException("The delimiter can't be null or empty");
        }

        if (originStr.toUpperCase().startsWith("\\X") || originStr.toUpperCase().startsWith("0X")) {
            String hexStr = originStr.substring(2);
            // check hex str
            if (hexStr.isEmpty()) {
                throw new RuntimeException("Invalid delimiter '" + originStr + ": empty hex string");
            }
            if (hexStr.length() % 2 != 0) {
                throw new RuntimeException("Invalid delimiter '" + originStr + ": hex length must be a even number");
            }
            for (char hexChar : hexStr.toUpperCase().toCharArray()) {
                if (HEX_STRING.indexOf(hexChar) == -1) {
                    throw new RuntimeException("Invalid delimiter '" + originStr + "': invalid hex format");
                }
            }

            // transform to delimiter
            StringWriter writer = new StringWriter();
            for (byte b : hexStrToBytes(hexStr)) {
                writer.append((char) b);
            }
            return writer.toString();
        } else {
            return originStr;
        }
    }
}
