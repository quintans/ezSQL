package com.github.quintans.ezSQL.orm.mapper;

import com.github.quintans.ezSQL.transformers.MapBeanTransformer;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class BuilderMapper<T> extends MapBeanTransformer<T> {

    public BuilderMapper(Class<T> clazz) {
        super(clazz);
    }

    @Override
    protected Object create(Class<?> type) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Object builder = call(type, null,"newBuilder"); // static method
        return call(builder.getClass(), builder,"build");
    }

    private Object call(Class<?> type, Object instance, String name) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = type.getDeclaredMethod(name);
        if (m == null) {
            throw new PersistenceException(type.getCanonicalName() + " does not have the method " + name);
        }
        m.setAccessible(true);
        return m.invoke(instance);
    }
}