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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ErrorLogSanitizationTest {

    // Test data class for parameterized tests
    public static class SanitizationTestCase {
        public final String name;
        public final String input;
        public final String[] shouldNotContain;
        public final String[] shouldContain;
        public final Integer expectedLineCount;

        public SanitizationTestCase(String name, String input, String[] shouldNotContain, String[] shouldContain) {
            this(name, input, shouldNotContain, shouldContain, null);
        }

        public SanitizationTestCase(String name, String input, String[] shouldNotContain, String[] shouldContain, Integer expectedLineCount) {
            this.name = name;
            this.input = input;
            this.shouldNotContain = shouldNotContain;
            this.shouldContain = shouldContain;
            this.expectedLineCount = expectedLineCount;
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testCases() {
        return Arrays.asList(new Object[][]{
            {new SanitizationTestCase(
                "Mixed sensitive data with various Row formats",
                "Error: Value ''1753389251'' is out of range. The type of 'src_created_ts' is DATETIME. Row: [1328285340, '9411824179105899', '9000000947990669', '2025-02-25 00:00:00', 1, '055f79f2-8f22-4081-9a4d-792d2bd73400', 'QBL Assisted Bookkeeping', 1, 2167, 54, '1753389251', '1328285340', '1753389251', 1096, 718, 1753389251174, 0]\n" +
                "Error: Invalid argument: Value length is beyond the capacity. column=test_string_column, capacity=26\n" +
                "be/src/exec/json_scanner.cpp:588 _construct_column(val, column.get(), _prev_parsed_position[key_index].type, _prev_parsed_position[key_index].key). Row: {\"auth_id\":\"1234567890\",\"ivid\":\"28b14cdc-5b59-464e-a8dd-1esdasdk\",\"page_url\":\"/app/test\",\"action\":\"clicked\",\"company_id\":\"1234567890\",\"consented\":true,\"object\":\"report\",\"object_detail\":\"abc\",\"org\":\"new\",\"purpose\":\"1\",\"scope\":\"reports\",\"scope_area\":\"reports\",\"test_string_column\":\"abcdefghijklmnopqrtuvwxyzaa\",\"ui_action\":\"clicked\",\"ui_object\":\"link\",\"ui_object_detail\":\"BAL_SHEET\",\"timestamp\":\"2024-10-23T21:16:27.198Z\",\"session_id\":\"03646ad8-3ea3-3a92-a1f4-hfbehbfcw\",\"ingestion_datetime\":\"2025-08-03T15:18:33.408+0530\"}\n" +
                "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, NULL, '1234567890', 'new', '4', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']",
                new String[]{"055f79f2-8f22-4081-9a4d-792d2bd73400", "9411824179105899", "QBL Assisted Bookkeeping", "[1328285340,", "Row:", "1753389251", "auth_id\":\"1234567890", "03646ad8-3ea3-3a92-a1f4-hfbehbfcw"},
                new String[]{"Error: column value is out of range", "src_created_ts", "DATETIME", "Error: Invalid argument: Value length is beyond the capacity", "Error: NULL value in non-nullable column"}
            )},

            {new SanitizationTestCase(
                "NULL constraint violations",
                "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, NULL, '1234567890', 'new', '4', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']\n" +
                "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, NULL, '1234567890', 'new', '5', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']",
                new String[]{"03646ad8-3ea3-3a92-a1f4-hfbehbfcw", "1234567890", "[NULL, '03646ad8", "2024-10-23 21:16:27.198000", "Row:"},
                new String[]{"Error: NULL value in non-nullable column", "auth_id"},
                2
            )},

            {new SanitizationTestCase(
                "JSON Row data on single line",
                "be/src/exec/json_scanner.cpp:588 _construct_column(...). Row: {\"auth_id\":\"1234567890\",\"session_id\":\"03646ad8-3ea3-3a92-a1f4-hfbehbfcw\",\"data\":\"sensitive\"}\n" +
                "Error: Next error here.",
                new String[]{"auth_id", "session_id", "{", "Row:"},
                new String[]{"be/src/exec/json_scanner.cpp:588", "Error: Next error here."}
            )},

            {new SanitizationTestCase(
                "Mixed quote formats in column values",
                "Error: Value ''double-single-quotes'' is invalid.\n" +
                "Error: Value 'single-quotes' is invalid.\n" +
                "Error: Value \"double-quotes\" is invalid.\n" +
                "Error: Mixed Row: {\"key\":\"value\"} data.",
                new String[]{"double-single-quotes", "single-quotes", "double-quotes", "{"},
                new String[]{"Error: column value is invalid.", "Error: Mixed"}
            )},

            {new SanitizationTestCase(
                "Row in middle of line",
                "Some text Row: [data1, data2] more text after row\n" +
                "Another line Row: {\"json\":\"data\"} with text after",
                new String[]{"data1", "json", "Row:"},
                new String[]{"Some text", "Another line"}
            )},

            {new SanitizationTestCase(
                "Mixed line endings",
                "Error: Line 1. Row: [data1]\r\n" +
                "Error: Line 2. Row: [data2]\r" +
                "Error: Line 3. Row: [data3]\n",
                new String[]{"data1", "data2", "data3"},
                new String[]{"Error: Line 1.", "Error: Line 2.", "Error: Line 3."},
                3
            )},

            {new SanitizationTestCase(
                "Real world example",
                "Error: Invalid argument: Value length is beyond the capacity. column=test_string_column, capacity=26\n" +
                "be/src/formats/json/nullable_column.cpp:226 add_binary_column(data_column.get(), type_desc, name, value)\n" +
                "be/src/exec/json_scanner.cpp:588 _construct_column(val, column.get(), _prev_parsed_position[key_index].type, _prev_parsed_position[key_index].key). Row: {\"auth_id\":\"1234567890\",\"ivid\":\"28b14cdc-5b59-464e-a8dd-1esdasdk\",\"page_url\":\"/app/test\",\"action\":\"clicked\",\"company_id\":\"1234567890\",\"consented\":true,\"object\":\"report\",\"object_detail\":\"abc\",\"org\":\"new\",\"purpose\":\"2\",\"scope\":\"reports\",\"scope_area\":\"reports\",\"test_string_column\":\"abcdefghijklmnopqrtuvwxyzaa\",\"ui_action\":\"clicked\",\"ui_object\":\"link\",\"ui_object_detail\":\"BAL_SHEET\",\"timestamp\":\"2024-10-23T21:16:27.198Z\",\"session_id\":\"03646ad8-3ea3-3a92-a1f4-hfbehbfcw\",\"ingestion_datetime\":\"2025-08-03T15:30:00.658+0530\"}\n" +
                "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, 'abcdefghijklmnopqrtuvwxyz', NULL, '1234567890', 'new', '5', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']",
                new String[]{"1234567890", "03646ad8-3ea3-3a92-a1f4-hfbehbfcw"},
                new String[]{"Error: Invalid argument: Value length is beyond the capacity", "column=test_string_column", "be/src/formats/json/nullable_column.cpp:226", "Error: NULL value in non-nullable column 'auth_id'"}
            )},

            {new SanitizationTestCase(
                "Empty lines handling",
                "Error: First error.\n" +
                "\n" +
                "\n" +
                "Error: Second error with Row: [sensitive-data]\n" +
                "\n" +
                "Third line.",
                new String[]{"sensitive-data"},
                new String[]{"Error: First error.", "Error: Second error with", "Third line."}
            )}
        });
    }

    @Parameterized.Parameter
    public SanitizationTestCase testCase;

    @Test
    public void testSanitization() {
        String sanitized = StreamLoadUtils.sanitizeErrorLog(testCase.input);

        // Verify sensitive data is removed
        for (String shouldNotContain : testCase.shouldNotContain) {
            Assert.assertFalse("Should not contain: " + shouldNotContain, 
                sanitized.contains(shouldNotContain));
        }

        // Verify useful debugging info is preserved
        for (String shouldContain : testCase.shouldContain) {
            Assert.assertTrue("Should contain: " + shouldContain, 
                sanitized.contains(shouldContain));
        }

        // Check line count if specified
        if (testCase.expectedLineCount != null) {
            String[] lines = sanitized.split("[\r\n]+");
            Assert.assertEquals("Should have " + testCase.expectedLineCount + " lines", 
                testCase.expectedLineCount.intValue(), lines.length);
        }
    }

    // Individual tests for edge cases that need special logic
    @Test
    public void testSanitizeErrorLogWithNullOrEmpty() {
        String sanitized = StreamLoadUtils.sanitizeErrorLog(null);
        Assert.assertNull("Null input should return null", sanitized);
        Assert.assertEquals("Empty input should return empty", "", StreamLoadUtils.sanitizeErrorLog(""));
        Assert.assertEquals("Whitespace should return as-is", "   ", StreamLoadUtils.sanitizeErrorLog("   "));
    }

    @Test
    public void testSanitizeErrorLogWithNonRowErrors() {
        String nonRowError = "Connection timeout\nInvalid configuration\nOther system error";
        String sanitized = StreamLoadUtils.sanitizeErrorLog(nonRowError);
        
        // Non-row errors should be preserved as-is
        Assert.assertEquals("Non-row errors should be preserved", nonRowError, sanitized);
    }

    @Test
    public void testSanitizeErrorLogWithOnlyRowData() {
        // Test log with only row data
        String errorLog = "Row: [1234567890, 'sensitive-data', 'more-data']";

        String sanitized = StreamLoadUtils.sanitizeErrorLog(errorLog);

        // Should return default message when everything is sanitized
        Assert.assertEquals("Should return default message", 
                "Data validation errors detected. Row data has been redacted for security.", 
                sanitized);
    }

    @Test
    public void testSanitizeErrorLogWithManyErrors() {
        // Create error log with many errors - should NOT be summarized anymore
        StringBuilder manyErrors = new StringBuilder();
        for (int i = 0; i < 15; i++) {
            manyErrors.append("Error: Value ''175338925").append(i).append("'' is out of range. The type of 'src_created_ts' is DATETIME. Row: [123456").append(i).append(", 'sensitive-data-").append(i).append("']\n");
        }

        String sanitized = StreamLoadUtils.sanitizeErrorLog(manyErrors.toString());

        // Should NOT generate summary - should process each error individually
        Assert.assertFalse("Should not contain summary message", sanitized.contains("Multiple data validation errors detected"));
        Assert.assertFalse("Should not contain sensitive data", sanitized.contains("sensitive-data-"));
        Assert.assertFalse("Should not contain Row sections", sanitized.contains("Row:"));
        Assert.assertTrue("Should contain individual error lines", sanitized.contains("Error: column value is out of range"));
        
        // Count lines to ensure all errors are processed individually
        String[] lines = sanitized.split("\n");
        Assert.assertEquals("Should have 15 individual error lines", 15, lines.length);
    }

    @Test
    public void testSanitizeErrorLogWithSpecialCharactersInJson() {
        // Test JSON with special characters that might affect regex
        String errorLog = "Row: {\"data\":\"value with \\\"quotes\\\"\",\"pattern\":\".*[]{}\"}";

        String sanitized = StreamLoadUtils.sanitizeErrorLog(errorLog);

        // Should handle special characters and remove all data
        Assert.assertEquals("Should return default message", 
                "Data validation errors detected. Row data has been redacted for security.", 
                sanitized);
    }
} 