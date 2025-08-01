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

public class ErrorLogSanitizationTest {

    @Test
    public void testSanitizeErrorLogWithSensitiveData() {
        // Sample error log with sensitive data (similar to user's report)
        String rawErrorLog = "Error: Value ''1753389251'' is out of range. The type of 'src_created_ts' is DATETIME. Row: [1328285340, '9411824179105899', '9000000947990669', '2025-02-25 00:00:00', 1, '055f79f2-8f22-4081-9a4d-792d2bd73400', 'QBL Assisted Bookkeeping', 1, 2167, 54, '1753389251', '1328285340', '1753389251', 1096, 718, 1753389251174, 0]\n" +
                "Error: Value ''1753389251'' is out of range. The type of 'src_created_ts' is DATETIME. Row: [264251386, '9411824179105899', '9000000534951350', '2025-03-29 00:00:00', 0, 'ae65a287-6fc4-4c5f-8e4e-849d80b61e6d', 'QBL Assisted Bookkeeping', 1, 2229, 72, '1753389251', '264251386', '1753389251', 775, 371, 1753389251094, 0]\n" +
                "Error: Value ''1753389255'' is out of range. The type of 'src_created_ts' is DATETIME. Row: [4212244691, '9411824179105899', '9000000646475679', '2024-07-19 00:00:00', 1, '234f1c65-8ff4-4edf-a1f5-4df9286810c4', 'QBL Assisted Bookkeeping', 0, 5293, 76, '1753389255', '4212244691', '1753389255', 1491, 817, 1753389255230, 0]";

        String sanitized = StreamLoadUtils.sanitizeErrorLog(rawErrorLog);

        // Verify sensitive data is removed
        Assert.assertFalse("Should not contain sensitive UUID", sanitized.contains("055f79f2-8f22-4081-9a4d-792d2bd73400"));
        Assert.assertFalse("Should not contain sensitive ID", sanitized.contains("9411824179105899"));
        Assert.assertFalse("Should not contain business name", sanitized.contains("QBL Assisted Bookkeeping"));
        Assert.assertFalse("Should not contain actual row data", sanitized.contains("[1328285340,"));
        Assert.assertFalse("Should not contain Row section", sanitized.contains("Row:"));
        Assert.assertFalse("Should not contain specific column values", sanitized.contains("1753389251"));
        
        // Verify useful debugging info is preserved
        Assert.assertTrue("Should contain generic error type", sanitized.contains("Error: column value is out of range"));
        Assert.assertTrue("Should contain column info", sanitized.contains("src_created_ts"));
        Assert.assertTrue("Should contain data type info", sanitized.contains("DATETIME"));
        Assert.assertFalse("Should not contain redaction notice", sanitized.contains("DATA_REDACTED_FOR_SECURITY"));
    }

    @Test
    public void testSanitizeErrorLogWithActualNullConstraintErrors() {
        // Actual error log format provided by user - NULL constraint violations
        String actualErrorLog = "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, NULL, '1234567890', 'new', '4', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']\n" +
                "Error: NULL value in non-nullable column 'auth_id'. Row: [NULL, '03646ad8-3ea3-3a92-a1f4-hfbehbfcw', 2024-10-23 21:16:27.198000, 1, NULL, '1234567890', 'new', '5', 'reports', 'reports', NULL, 'clicked', 'report', 'abc', 'clicked', 'link', 'BAL_SHEET']";

        String sanitized = StreamLoadUtils.sanitizeErrorLog(actualErrorLog);

        // Verify sensitive data is completely removed
        Assert.assertFalse("Should not contain UUID", sanitized.contains("03646ad8-3ea3-3a92-a1f4-hfbehbfcw"));
        Assert.assertFalse("Should not contain phone number", sanitized.contains("1234567890"));
        Assert.assertFalse("Should not contain actual row data", sanitized.contains("[NULL, '03646ad8"));
        Assert.assertFalse("Should not contain timestamp data", sanitized.contains("2024-10-23 21:16:27.198000"));
        Assert.assertFalse("Should not contain Row section", sanitized.contains("Row:"));
        
        // Verify useful debugging info is preserved
        Assert.assertTrue("Should contain error type", sanitized.contains("Error: NULL value in non-nullable column"));
        Assert.assertTrue("Should contain column name", sanitized.contains("auth_id"));
        Assert.assertFalse("Should not contain redaction notice", sanitized.contains("DATA_REDACTED_FOR_SECURITY"));
        
        // Verify structure is clean
        String[] lines = sanitized.split("\n");
        Assert.assertEquals("Should have 2 error lines", 2, lines.length);
        for (String line : lines) {
            Assert.assertFalse("No line should contain Row data", line.contains("Row:"));
            Assert.assertTrue("Each line should end with period", line.trim().endsWith("."));
        }
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
} 