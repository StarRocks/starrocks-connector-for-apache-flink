package com.starrocks.connector.flink.tools;

import java.util.HashSet;
import java.util.Set;

public class ClassUtils {
    private static final Set<Class<?>> wrapperPrimitives = new HashSet<>();

    static {
        wrapperPrimitives.add(Boolean.class);
        wrapperPrimitives.add(Byte.class);
        wrapperPrimitives.add(Character.class);
        wrapperPrimitives.add(Short.class);
        wrapperPrimitives.add(Integer.class);
        wrapperPrimitives.add(Long.class);
        wrapperPrimitives.add(Double.class);
        wrapperPrimitives.add(Float.class);
        wrapperPrimitives.add(Void.TYPE);
    }

    public static boolean isPrimitiveWrapper(Class<?> type) {
        return wrapperPrimitives.contains(type);
    }
}
