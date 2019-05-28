package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.common.api.Convert;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

public class QueryMapperBean implements QueryMapper {

    @Override
    public boolean support(Class<?> rootClass) {
        return true;
    }

    @Override
    public Object createRoot(Class<?> rootClass) {
        return create(rootClass);
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        try {
            TypedField tf = FieldUtils.getBeanTypedField(parentInstance.getClass(), name);
            if (tf != null) {
                Class<?> type = tf.getPropertyType();

                // if it is a collection we create an instance of the subtype and add it to the collection
                // we return the subtype and not the collection
                if (Collection.class.isAssignableFrom(type)) {
                    type = FieldUtils.getTypeGenericClass(tf.getType());
                }
                return create(type);
            } else {
                throw new PersistenceException(parentInstance.getClass() + " does not have setter for " + name);
            }
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }

    protected Object create(Class<?> type) {
        try {
            Constructor<?> constructor = type.getDeclaredConstructor();
            if (constructor == null) {
                throw new PersistenceException(type.getCanonicalName() + " does not have an empty constructor.");
            }
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void link(Object parentInstance, String name, Object value) {
        try {
            TypedField tf = FieldUtils.getBeanTypedField(parentInstance.getClass(), name);
            if (tf != null) {
                Class<?> type = tf.getPropertyType();

                // if it is a collection we create an instance of the subtype and add it to the collection
                // we return the subtype and not the collection
                if (Collection.class.isAssignableFrom(type)) {
                    Collection collection = (Collection) tf.get(parentInstance);
                    if (collection == null) {
                        collection = new LinkedHashSet<>();
                        tf.set(parentInstance, collection);
                    }
                    collection.add(value);
                    tf.set(parentInstance, collection);
                } else {
                    tf.set(parentInstance, value);
                }
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean map(Row row, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;
            for (MapColumn mapColumn : mapColumns) {

                TypedField tf = FieldUtils.getBeanTypedField(instance.getClass(), mapColumn.getAlias());
                if (tf != null) {

                    Convert convert = tf.getField().getAnnotation(Convert.class);
                    Object value;
                    if(convert != null) {
                        value = row.get(mapColumn.getIndex());
                        value = convert.value().newInstance().fromDb(value);
                    } else {
                        Class<?> type = tf.getPropertyType();
                        value = row.get(mapColumn.getIndex(), type);
                    }
                    tf.set(instance, value);
                    touched |= value != null;
                }
            }

            return touched;
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new PersistenceException(e);
        }
    }
}
