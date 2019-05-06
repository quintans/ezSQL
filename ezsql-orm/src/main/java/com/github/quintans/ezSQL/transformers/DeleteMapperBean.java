package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public class DeleteMapperBean implements DeleteMapper {

    @Override
    public boolean support(Class<?> rootClass) {
        return true;
    }

    @Override
    public Result<Object> map(Column column, Object object) {
        String alias = column.getAlias();
        TypedField tf = FieldUtils.getBeanTypedField(object.getClass(), alias);
        if (tf != null) {
            Object o;
            try {
                o = tf.get(object);
            } catch (Exception e) {
                throw new PersistenceException("Unable to read from " + object.getClass().getSimpleName() + "." + alias, e);
            }
            return Result.of(o);
        }
        return Result.fail();
    }
}
