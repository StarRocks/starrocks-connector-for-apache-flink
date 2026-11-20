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

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import com.starrocks.data.load.stream.auth.StaticCredentialsProvider;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarRocksSinkOptionsCredentialsProviderTest {

    /** Named by {@code sink.credentials-provider.class}; counts how often it is constructed. */
    public static class CountingProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;
        static final AtomicInteger INSTANCES = new AtomicInteger();

        public CountingProvider() {
            INSTANCES.incrementAndGet();
        }

        @Override
        public Credentials getCredentials() {
            return new Credentials("from_class", "class_password");
        }
    }

    /** Not a provider; records whether its static initializer ran. */
    public static class NotAProvider {
        static {
            INITIALIZED_BY_STATIC_BLOCK.incrementAndGet();
        }
    }

    static final AtomicInteger INITIALIZED_BY_STATIC_BLOCK = new AtomicInteger();

    /** A provider whose constructor fails, as one reading a missing secret would. */
    public static class FailingProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;

        public FailingProvider() {
            throw new IllegalStateException("secret file missing");
        }

        @Override
        public Credentials getCredentials() {
            return null;
        }
    }

    private static CredentialsProvider providerForClassOption(String className) {
        return base()
                .withProperty(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key(), className)
                .build()
                .getCredentialsProvider();
    }

    @Before
    public void setUp() {
        CountingProvider.INSTANCES.set(0);
        new MockUp<StarRocksSinkTable>() {
            @Mock
            public String getVersion() {
                return "3.3.0";
            }
        };
    }

    private static StarRocksSinkOptions.Builder base() {
        return StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                .withProperty("load-url", "127.0.0.1:8030")
                .withProperty("database-name", "db")
                .withProperty("table-name", "t")
                .withProperty("username", "root")
                .withProperty("password", "pw")
                .withProperty("sink.properties.format", "json");
    }

    private static StreamLoadProperties streamLoadProperties(StarRocksSinkOptions options) {
        return options.getProperties(StarRocksSinkTable.builder().sinkOptions(options).build());
    }

    @Test
    public void testUsernameAndPasswordAreUsedWithoutProvider() {
        StarRocksSinkOptions options = base().build();

        assertNull(options.getCredentialsProvider());
        CredentialsProvider provider = streamLoadProperties(options).getCredentialsProvider();
        assertTrue(provider instanceof StaticCredentialsProvider);
        assertEquals(new Credentials("root", "pw"), provider.getCredentials());
    }

    @Test
    public void testBuilderProviderIsPassedToStreamLoadProperties() {
        CredentialsProvider provider = () -> new Credentials("rotated", "secret");
        StarRocksSinkOptions options = base().withCredentialsProvider(provider).build();

        assertSame(provider, streamLoadProperties(options).getCredentialsProvider());
    }

    @Test
    public void testBuilderProviderSurvivesSerializationOfOptions() throws Exception {
        StarRocksSinkOptions options = base()
                .withCredentialsProvider(new StaticCredentialsProvider("rotated", "secret"))
                .build();

        StarRocksSinkOptions copy = InstantiationUtil.clone(options);

        assertEquals(new Credentials("rotated", "secret"), copy.getCredentialsProvider().getCredentials());
    }

    @Test
    public void testClassOptionIsInstantiatedOnFirstUse() {
        StarRocksSinkOptions options = base()
                .withProperty(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key(), CountingProvider.class.getName())
                .build();

        CredentialsProvider provider = streamLoadProperties(options).getCredentialsProvider();
        assertEquals(0, CountingProvider.INSTANCES.get());

        assertEquals(new Credentials("from_class", "class_password"), provider.getCredentials());
        provider.getCredentials();
        assertEquals(1, CountingProvider.INSTANCES.get());
    }

    @Test
    public void testClassOptionIsInstantiatedAfterDeserialization() throws Exception {
        StarRocksSinkOptions options = base()
                .withProperty(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key(), CountingProvider.class.getName())
                .build();
        StreamLoadProperties properties = streamLoadProperties(options);

        StreamLoadProperties copy = InstantiationUtil.clone(properties);

        assertEquals(0, CountingProvider.INSTANCES.get());
        assertEquals(new Credentials("from_class", "class_password"), copy.getCredentialsProvider().getCredentials());
        assertEquals(1, CountingProvider.INSTANCES.get());
    }

    @Test
    public void testClassOptionRejectsClassThatIsNotAProvider() {
        StarRocksSinkOptions options = base()
                .withProperty(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key(), String.class.getName())
                .build();
        CredentialsProvider provider = options.getCredentialsProvider();

        try {
            provider.getCredentials();
            fail("a class that does not implement CredentialsProvider should be rejected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(String.class.getName()));
        }
    }

    @Test
    public void testClassOptionDoesNotInitializeClassThatIsNotAProvider() {
        CredentialsProvider provider = providerForClassOption(NotAProvider.class.getName());

        try {
            provider.getCredentials();
            fail("a class that does not implement CredentialsProvider should be rejected");
        } catch (IllegalArgumentException e) {
            assertEquals(0, INITIALIZED_BY_STATIC_BLOCK.get());
        }
    }

    @Test
    public void testClassOptionReportsConstructorFailure() {
        CredentialsProvider provider = providerForClassOption(FailingProvider.class.getName());

        try {
            provider.getCredentials();
            fail("a constructor that throws should be reported");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("constructor"));
            assertEquals("secret file missing", e.getCause().getMessage());
        }
    }

    @Test
    public void testClassOptionReportsMissingClass() {
        CredentialsProvider provider = providerForClassOption("com.example.DoesNotExist");

        try {
            provider.getCredentials();
            fail("a missing class should be reported");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("was not found"));
        }
    }

    @Test
    public void testBuilderRejectsProviderTogetherWithClassOption() {
        StarRocksSinkOptions.Builder builder = base()
                .withProperty(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key(), CountingProvider.class.getName())
                .withCredentialsProvider(new StaticCredentialsProvider("rotated", "secret"));

        try {
            builder.build();
            fail("a provider and the class option should not be accepted together");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS.key()));
        }
    }

    @Test
    public void testSinkVersionV1RejectsProvider() {
        StarRocksSinkOptions options = base()
                .withProperty(StarRocksSinkOptions.SINK_VERSION.key(), "V1")
                .withCredentialsProvider(new StaticCredentialsProvider("rotated", "secret"))
                .build();

        try {
            SinkFunctionFactory.getSinkVersion(options);
            fail("sink version V1 does not use the credentials provider and should reject it");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("V2"));
        }
    }

    @Test
    public void testClassOptionRegisteredWithTableFactory() {
        assertTrue(new StarRocksDynamicTableSinkFactory().optionalOptions()
                .contains(StarRocksSinkOptions.SINK_CREDENTIALS_PROVIDER_CLASS));
    }
}
