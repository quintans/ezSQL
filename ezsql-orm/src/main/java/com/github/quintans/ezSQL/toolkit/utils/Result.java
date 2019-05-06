package com.github.quintans.ezSQL.toolkit.utils;

import java.util.function.Consumer;

public class Result<T> {
    private boolean success;
    private T value;

    public static <R> Result<R> of(R value) {
        return new Result<>(value, true);
    }

    public static <R> Result<R> fail() {
        return new Result<>(null, false);
    }

    public static <R> Result<R> fail(R value) {
        return new Result<>(value, false);
    }

    public Result(T value, boolean success) {
        this.success = success;
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
    }

    public T get() {
        return value;
    }

    public void onSuccess(Consumer<T> consumer){
        if(this.success) {
            consumer.accept(value);
        }
    }

    public void onFailure(Consumer<T> consumer){
        if(!this.success) {
            consumer.accept(value);
        }
    }
}
