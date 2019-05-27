package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.Convert;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.InvocationTargetException;

public class UpdateMapperBean implements UpdateMapper {
    @Override
    public boolean support(Class<?> rootClass) {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result<UpdateValue> map(Driver driver, Column column, Object object) {
        String alias = column.getAlias();
        TypedField tf = FieldUtils.getBeanTypedField(object.getClass(), alias);
        if (tf != null) {
            Object o;
            try {
                o = tf.get(object);
                Convert convert = tf.getField().getAnnotation(Convert.class);
                if(convert != null) {
                    o = driver.getConverter(convert.value()).toDb(o);
                }
                return Result.of(new UpdateValue(o, v -> {
                    try {
                        tf.set(object, v);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new PersistenceException("Unable to set Version data.", e);
                    }
                }));
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new PersistenceException("Unable to read from " + object.getClass().getSimpleName() + "." + alias, e);
            }
        }
        return Result.fail();
    }

    @Override
    public Object newVersion(Object version) {
        // version increment
        if (Long.class.isAssignableFrom(version.getClass())) {
            return (Long) version + 1L;
        } else {
            return (Integer) version + 1;
        }
    }
}
