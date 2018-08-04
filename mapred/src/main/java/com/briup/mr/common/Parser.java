package com.briup.mr.common;

import java.util.Optional;
import java.util.function.Supplier;

public interface Parser<T, U extends Parser<T, U>> {
    default Optional<U> parse(Supplier<T> line) {
        parse(line.get());
        return isValid() ? Optional.of((U) this) : Optional.empty();

    }

    void parse(T t);

    boolean isValid();
}
