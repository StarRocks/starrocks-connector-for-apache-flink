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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;
import com.starrocks.data.load.stream.auth.StaticCredentialsProvider;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class StreamLoadPropertiesCredentialsTest {

    /** A provider with state, to check that the state survives serialization. */
    static class FixedProvider implements CredentialsProvider {
        private static final long serialVersionUID = 1L;
        private final String username;
        private final String password;

        FixedProvider(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public Credentials getCredentials() {
            return new Credentials(username, password);
        }
    }

    private static StreamLoadProperties.Builder baseBuilder() {
        StreamLoadTableProperties tableProps = StreamLoadTableProperties.builder()
                .database("db")
                .table("tbl")
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .build();
        return StreamLoadProperties.builder()
                .loadUrls("127.0.0.1:8030")
                .defaultTableProperties(tableProps);
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T object) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(object);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }

    @Test
    public void testDefaultProviderReturnsBuilderUsernameAndPassword() {
        StreamLoadProperties properties = baseBuilder().username("root").password("pw").build();

        assertTrue(properties.getCredentialsProvider() instanceof StaticCredentialsProvider);
        assertEquals(new Credentials("root", "pw"), properties.getCredentialsProvider().getCredentials());
        assertEquals("root", properties.getUsername());
        assertEquals("pw", properties.getPassword());
    }

    @Test
    public void testDefaultProviderProducesTheSameHeaderAsUsernameAndPassword() {
        StreamLoadProperties properties = baseBuilder().username("root").password("pw").build();

        String header = StreamLoadUtils.getBasicAuthHeader(properties.getCredentialsProvider().getCredentials());

        assertEquals(StreamLoadUtils.getBasicAuthHeader("root", "pw"), header);
        assertEquals("Basic cm9vdDpwdw==", header);
    }

    @Test
    public void testCustomProviderTakesPrecedenceOverUsernameAndPassword() {
        FixedProvider provider = new FixedProvider("rotated", "secret");
        StreamLoadProperties properties = baseBuilder()
                .username("root")
                .password("pw")
                .credentialsProvider(provider)
                .build();

        assertSame(provider, properties.getCredentialsProvider());
        assertEquals(new Credentials("rotated", "secret"), properties.getCredentialsProvider().getCredentials());
    }

    @Test
    public void testCustomProviderSurvivesJavaSerialization() throws Exception {
        StreamLoadProperties properties = baseBuilder()
                .credentialsProvider(new FixedProvider("rotated", "secret"))
                .build();

        StreamLoadProperties copy = roundTrip(properties);

        assertNotSame(properties.getCredentialsProvider(), copy.getCredentialsProvider());
        assertEquals(new Credentials("rotated", "secret"), copy.getCredentialsProvider().getCredentials());
    }

    @Test
    public void testDefaultProviderSurvivesJavaSerialization() throws Exception {
        StreamLoadProperties properties = baseBuilder().username("root").password("pw").build();

        StreamLoadProperties copy = roundTrip(properties);

        assertEquals(new Credentials("root", "pw"), copy.getCredentialsProvider().getCredentials());
    }

    @Test
    public void testJsonOfPropertiesOmitsProviderAndPassword() throws Exception {
        StreamLoadProperties properties = baseBuilder()
                .username("root")
                .password("pw-not-logged")
                .credentialsProvider(new FixedProvider("rotated", "secret-not-logged"))
                .build();

        String json = new ObjectMapper().writeValueAsString(properties);

        assertFalse(json, json.contains("credentialsProvider"));
        assertFalse(json, json.contains("not-logged"));
    }

    @Test
    public void testCredentialsToStringHidesPassword() {
        String text = new Credentials("root", "secret").toString();

        assertTrue(text, text.contains("root"));
        assertFalse(text, text.contains("secret"));
    }
}
