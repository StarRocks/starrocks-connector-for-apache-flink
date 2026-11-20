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

import com.starrocks.data.load.stream.auth.Credentials;
import com.starrocks.data.load.stream.auth.CredentialsProvider;

import java.lang.reflect.InvocationTargetException;

/**
 * Delegates to the {@link CredentialsProvider} named by {@code sink.credentials-provider.class}.
 *
 * <p>Only the class name is serialized with the job. The named class is instantiated through its
 * public no-argument constructor on first use, which is on the task manager, so it does not need
 * to be constructible on the client.
 */
class ClassNameCredentialsProvider implements CredentialsProvider {

    private static final long serialVersionUID = 1L;

    private final String className;
    private transient volatile CredentialsProvider delegate;

    ClassNameCredentialsProvider(String className) {
        this.className = className;
    }

    String getClassName() {
        return className;
    }

    @Override
    public Credentials getCredentials() {
        CredentialsProvider provider = delegate;
        if (provider == null) {
            synchronized (this) {
                provider = delegate;
                if (provider == null) {
                    provider = instantiate();
                    delegate = provider;
                }
            }
        }
        return provider.getCredentials();
    }

    private CredentialsProvider instantiate() {
        Class<?> clazz = loadClass();
        // The class is loaded without initialization, so a class name from a table option cannot
        // run the static initializer of an arbitrary class before its type is checked.
        if (!CredentialsProvider.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("'%s' does not implement %s",
                    className, CredentialsProvider.class.getName()));
        }
        try {
            return clazz.asSubclass(CredentialsProvider.class).getConstructor().newInstance();
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(String.format(
                    "The constructor of credentials provider '%s' threw an exception.", className), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(String.format(
                    "Failed to instantiate credentials provider '%s'. It must be a public class with a " +
                            "public no-argument constructor.", className), e);
        }
    }

    private Class<?> loadClass() {
        // The context class loader is the user code class loader on a Flink task manager, which
        // can see a provider shipped in the job jar even when the connector is in Flink's lib.
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            try {
                return Class.forName(className, false, contextClassLoader);
            } catch (ClassNotFoundException e) {
                // fall back to the connector's own class loader below
            }
        }
        try {
            return Class.forName(className, false, ClassNameCredentialsProvider.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format(
                    "Credentials provider class '%s' was not found.", className), e);
        }
    }

    @Override
    public String toString() {
        return "ClassNameCredentialsProvider{className='" + className + "'}";
    }
}
