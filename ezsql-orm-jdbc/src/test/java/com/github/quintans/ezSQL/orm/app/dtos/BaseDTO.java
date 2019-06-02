package com.github.quintans.ezSQL.orm.app.dtos;

public class BaseDTO<T> {
    protected T id;

    public T getId() {
        return id;
    }

    public void setId(T id) {
        this.id = id;
    }

}
