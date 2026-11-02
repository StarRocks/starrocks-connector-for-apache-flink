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

package com.starrocks.connector.flink.examples.datastream;

import com.starrocks.data.load.stream.auth.Credentials;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KubernetesSecretCredentialsProviderTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static void writeFile(Path file, String content) throws IOException {
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    /** Writes a new version of the Secret and swaps {@code ..data} to it, as the kubelet does. */
    private static void publish(Path secretDir, String version, String username, String password) throws IOException {
        Path versionDir = Files.createDirectory(secretDir.resolve("..ts_" + version));
        writeFile(versionDir.resolve("username"), username + "\n");
        writeFile(versionDir.resolve("password"), password + "\n");
        Path tmpLink = secretDir.resolve("..data_tmp");
        Files.createSymbolicLink(tmpLink, versionDir.getFileName());
        Files.move(tmpLink, secretDir.resolve("..data"), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /** Removes a superseded version directory, as the kubelet does after swapping {@code ..data}. */
    private static void remove(Path versionDir) throws IOException {
        try (Stream<Path> paths = Files.walk(versionDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    @Test
    public void testReturnsNewCredentialsAfterSecretUpdate() throws Exception {
        Path secretDir = temporaryFolder.newFolder("secret").toPath();
        publish(secretDir, "1", "user_a", "password_a");
        KubernetesSecretCredentialsProvider provider = new KubernetesSecretCredentialsProvider(secretDir.toString());

        assertEquals(new Credentials("user_a", "password_a"), provider.getCredentials());

        publish(secretDir, "2", "user_b", "password_b");

        assertEquals(new Credentials("user_b", "password_b"), provider.getCredentials());
    }

    @Test
    public void testNeverReturnsTornPairWhileSecretIsUpdated() throws Exception {
        Path secretDir = temporaryFolder.newFolder("secret").toPath();
        publish(secretDir, "0", "user_0", "password_0");
        KubernetesSecretCredentialsProvider provider = new KubernetesSecretCredentialsProvider(secretDir.toString());
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicReference<String> torn = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                while (!done.get()) {
                    Credentials credentials = provider.getCredentials();
                    String suffix = credentials.getUsername().substring("user_".length());
                    if (!credentials.getPassword().equals("password_" + suffix)) {
                        torn.set(credentials.getUsername() + "/" + credentials.getPassword());
                    }
                }
            } catch (RuntimeException e) {
                torn.set("read failed: " + e + ", cause: " + e.getCause());
            }
        });
        reader.start();
        try {
            for (int i = 1; i <= 200; i++) {
                publish(secretDir, String.valueOf(i), "user_" + i, "password_" + i);
                remove(secretDir.resolve("..ts_" + (i - 1)));
            }
        } finally {
            done.set(true);
            reader.join();
        }

        assertNull(torn.get());
        assertEquals(new Credentials("user_200", "password_200"), provider.getCredentials());
    }

    @Test
    public void testReadsAgainWhenResolvedVersionIsRemovedBeforeRead() throws Exception {
        Path secretDir = temporaryFolder.newFolder("secret").toPath();
        publish(secretDir, "1", "user_a", "password_a");
        AtomicBoolean rotated = new AtomicBoolean(false);
        KubernetesSecretCredentialsProvider provider =
                new KubernetesSecretCredentialsProvider(secretDir.toString()) {
                    @Override
                    Credentials read(Path dir) {
                        if (rotated.compareAndSet(false, true)) {
                            try {
                                publish(secretDir, "2", "user_b", "password_b");
                                remove(dir);
                            } catch (IOException e) {
                                throw new AssertionError(e);
                            }
                        }
                        return super.read(dir);
                    }
                };

        assertEquals(new Credentials("user_b", "password_b"), provider.getCredentials());
    }

    @Test
    public void testFailsWhenSecretHasNoPassword() throws Exception {
        Path secretDir = temporaryFolder.newFolder("secret").toPath();
        publish(secretDir, "1", "user_a", "password_a");
        Files.delete(secretDir.resolve("..ts_1").resolve("password"));
        KubernetesSecretCredentialsProvider provider = new KubernetesSecretCredentialsProvider(secretDir.toString());

        try {
            provider.getCredentials();
            fail("a Secret without a password file should fail");
        } catch (UncheckedIOException e) {
            assertTrue(e.getMessage(), e.getMessage().endsWith("password"));
        }
    }

    @Test
    public void testSurvivesJavaSerialization() throws Exception {
        Path secretDir = temporaryFolder.newFolder("secret").toPath();
        publish(secretDir, "1", "user_a", "password_a");
        KubernetesSecretCredentialsProvider provider = new KubernetesSecretCredentialsProvider(secretDir.toString());
        provider.getCredentials();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(provider);
        }
        KubernetesSecretCredentialsProvider copy;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (KubernetesSecretCredentialsProvider) in.readObject();
        }

        assertEquals(new Credentials("user_a", "password_a"), copy.getCredentials());
    }

    @Test
    public void testReadsDirectoryWithoutDataLink() throws Exception {
        Path dir = temporaryFolder.newFolder("plain").toPath();
        writeFile(dir.resolve("username"), "user_a\n");
        writeFile(dir.resolve("password"), "password_a");
        KubernetesSecretCredentialsProvider provider = new KubernetesSecretCredentialsProvider(dir.toString());

        assertEquals(new Credentials("user_a", "password_a"), provider.getCredentials());

        writeFile(dir.resolve("password"), "password_b");

        assertEquals(new Credentials("user_a", "password_b"), provider.getCredentials());
    }

    @Test
    public void testNoArgumentConstructorRequiresEnvironmentVariable() {
        Assume.assumeTrue(System.getenv(KubernetesSecretCredentialsProvider.DIRECTORY_ENV) == null);
        try {
            new KubernetesSecretCredentialsProvider();
            fail("the no-argument constructor should fail when the environment variable is not set");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(KubernetesSecretCredentialsProvider.DIRECTORY_ENV));
        }
    }
}
