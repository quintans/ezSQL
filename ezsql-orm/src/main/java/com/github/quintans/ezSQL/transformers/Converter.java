package com.github.quintans.ezSQL.transformers;

public interface Converter<T, S> {
    S toDb(T beanValue);
    T fromDb(S dbValue);
}
