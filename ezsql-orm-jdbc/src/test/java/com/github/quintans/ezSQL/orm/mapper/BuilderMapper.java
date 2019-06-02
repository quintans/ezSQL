package com.github.quintans.ezSQL.orm.mapper;

import com.github.quintans.ezSQL.mapper.QueryMapperBean;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class BuilderMapper extends QueryMapperBean {

    private static final String NEW_BUILDER = "newBuilder";

    @Override
    public boolean support(Class<?> rootClass) {
        try {
            Method m = rootClass.getDeclaredMethod(NEW_BUILDER);
            int mod = m.getModifiers();
            return Modifier.isStatic(mod) && Modifier.isPublic(mod);
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @Override
    protected Object create(Class<?> type) {
        try {
            Object builder = call(type, null, NEW_BUILDER); // static method
            return call(builder.getClass(), builder, "build");
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new PersistenceException(e);
        }
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