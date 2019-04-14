package com.github.quintans.ezSQL.toolkit.reflection;


import java.lang.reflect.*;

public class TypedField {
    private Field field;
    private Method getter;
    private Method setter;
    private Type type;

    public TypedField(Field field, Method getter, Method setter, Type type) {
        this.field = field;
        this.getter = getter;
        this.setter = setter;
        this.type = type;
    }

    public Object get(Object obj) throws IllegalAccessException, InvocationTargetException {
        if(getter != null) {
            return getter.invoke(obj);
        } else {
            return field.get(obj);
        }
    }

    public void set(Object obj, Object value) throws IllegalAccessException, InvocationTargetException {
        if (setter != null) {
            setter.invoke(obj, value);
        } else {
            field.set(obj, value);
        }
    }

    public String getName() {
        return field.getName();
    }

    public Field getField() {
        return field;
    }

    public Type getType() {
        return type;
    }

    public boolean isGettable() {
        return getter != null;
    }

    public Class<?> getPropertyType() {
        if (type instanceof ParameterizedType) {
            ParameterizedType aType = (ParameterizedType) type;
            return (Class<?>) aType.getRawType();
        } else {
            return (Class<?>) type;
        }
    }

    @Override
    public String toString() {
        return "TypedField{" +
                "field=" + field.getName() +
                ", type=" + type +
                '}';
    }
}