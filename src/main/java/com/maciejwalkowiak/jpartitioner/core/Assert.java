package com.maciejwalkowiak.jpartitioner.core;

import org.jspecify.annotations.Nullable;

class Assert {

    public static void notNull(@Nullable Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
