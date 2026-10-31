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
import com.starrocks.data.load.stream.auth.CredentialsProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * An example {@link CredentialsProvider} that reads the user name and password from a Kubernetes
 * Secret mounted as a volume, so the sink picks up new credentials when the Secret is updated.
 *
 * <p>The mounted directory must contain the files {@code username} and {@code password}. This is
 * the case for an {@code Opaque} Secret with those two keys and for a
 * {@code kubernetes.io/basic-auth} Secret; the provider only reads the files and never looks at
 * the Secret type.
 *
 * <p>The kubelet updates a Secret volume by writing the new files to a new timestamped directory
 * and atomically switching the {@code ..data} symlink to it. The provider reads both files from
 * the directory {@code ..data} points to, so a user name is never paired with a password from a
 * different version of the Secret. The files are read again only when {@code ..data} points to a
 * different directory. When the kubelet removes that directory before its files are read, the
 * provider follows {@code ..data} to the newer one. When the directory has no {@code ..data}, for
 * example plain files on a local disk, the files are read from the directory itself on every call.
 *
 * <p>Use the constructor that takes the directory with the DataStream API. The no-argument
 * constructor, for the {@code sink.credentials-provider.class} option, reads the directory from
 * the {@value #DIRECTORY_ENV} environment variable of the task managers.
 */
public class KubernetesSecretCredentialsProvider implements CredentialsProvider {

    private static final long serialVersionUID = 1L;

    public static final String DIRECTORY_ENV = "STARROCKS_CREDENTIALS_DIR";

    private final String directory;
    private transient volatile CachedCredentials cache;

    public KubernetesSecretCredentialsProvider() {
        this(directoryFromEnvironment());
    }

    public KubernetesSecretCredentialsProvider(String directory) {
        this.directory = directory;
    }

    @Override
    public Credentials getCredentials() {
        Path dataLink = Paths.get(directory, "..data");
        if (!Files.isSymbolicLink(dataLink)) {
            return read(Paths.get(directory));
        }
        Path version = currentVersion(dataLink);
        while (true) {
            try {
                return readCached(version);
            } catch (UncheckedIOException e) {
                // The kubelet removes the previous version directory after it switches ..data, so a
                // version read just before an update can disappear. Retry only if ..data has moved on;
                // otherwise the file is missing from the Secret.
                Path latest = currentVersion(dataLink);
                if (!(e.getCause() instanceof NoSuchFileException) || latest.equals(version)) {
                    throw e;
                }
                version = latest;
            }
        }
    }

    private static Path currentVersion(Path dataLink) {
        try {
            return dataLink.resolveSibling(Files.readSymbolicLink(dataLink));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read the link " + dataLink, e);
        }
    }

    private Credentials readCached(Path version) {
        CachedCredentials current = cache;
        if (current == null || !current.source.equals(version)) {
            current = new CachedCredentials(version, read(version));
            cache = current;
        }
        return current.credentials;
    }

    Credentials read(Path dir) {
        return new Credentials(readFile(dir.resolve("username")), readFile(dir.resolve("password")));
    }

    private static String readFile(Path file) {
        try {
            String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            return content.replaceAll("[\\r\\n]+$", "");
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + file, e);
        }
    }

    static String directoryFromEnvironment() {
        String directory = System.getenv(DIRECTORY_ENV);
        if (directory == null || directory.isEmpty()) {
            throw new IllegalStateException("Environment variable " + DIRECTORY_ENV
                    + " must be set to the directory where the credentials Secret is mounted");
        }
        return directory;
    }

    private static final class CachedCredentials {
        private final Path source;
        private final Credentials credentials;

        private CachedCredentials(Path source, Credentials credentials) {
            this.source = source;
            this.credentials = credentials;
        }
    }
}
