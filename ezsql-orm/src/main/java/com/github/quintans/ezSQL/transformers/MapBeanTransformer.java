package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

public class MapBeanTransformer<T> implements QueryMapper<T> {
    private Class<T> clazz;

    public MapBeanTransformer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        try {
            // handling root table
            if (parentInstance == null) {
                return create(clazz);
            }

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
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            throw new PersistenceException(e);
        }
    }

    protected Object create(Class<?> type) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<?> constructor = type.getDeclaredConstructor();
        if(constructor == null) {
            throw new PersistenceException(type.getCanonicalName() + " does not have an empty constructor.");
        }
        constructor.setAccessible(true);
        return constructor.newInstance();
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

    @Override
    public boolean map(Record record, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;
            for (MapColumn mapColumn : mapColumns) {

                TypedField tf = FieldUtils.getBeanTypedField(instance.getClass(), mapColumn.getAlias());
                if (tf != null) {
                    Class<?> type = tf.getPropertyType();

                    Object value = record.get(mapColumn.getIndex(), type);
                    tf.set(instance, value);
                    touched |= value != null;
                }
            }

            return touched;
        } catch (IllegalAccessException | SQLException | InvocationTargetException e) {
            throw new PersistenceException(e);
        }
    }
}
