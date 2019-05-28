package com.github.quintans.ezSQL.mapper;

import java.util.function.Consumer;

public class UpdateValue {
    private Object current;
    private Consumer<Object> setter;

    public UpdateValue(Object current, Consumer<Object> setter) {
        this.current = current;
        this.setter = setter;
    }

    public Object getCurrent() {
        return current;
    }

    public Consumer<Object> getSetter() {
        return setter;
    }
}
