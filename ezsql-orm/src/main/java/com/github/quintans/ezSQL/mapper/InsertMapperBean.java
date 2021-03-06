package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.common.api.Convert;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("unchecked")
public class InsertMapperBean implements InsertMapper {
    @Override
    public boolean support(Class<?> rootClass) {
        return true;
    }

    @Override
    public Result<Object> map(Driver driver, Column<?> column, Object object, boolean versioned) {
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
                return Result.of(o);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new PersistenceException("Unable to read from " + object.getClass().getSimpleName() + "." + alias, e);
            }
        }
        return Result.fail();
    }
}
