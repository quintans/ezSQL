package com.github.quintans.ezSQL.common.api;

public interface Converter<T, S> {
    S toDb(T beanValue);
    T fromDb(S dbValue);
}
