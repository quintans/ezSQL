package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

public class MapBeanTransformer<T> implements QueryMapper {
    private Class<T> clazz;
    private Driver driver;

    public MapBeanTransformer(Class<T> clazz, Driver driver) {
        this.clazz = clazz;
        this.driver = driver;
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        try {
            // handling root table
            if (parentInstance == null) {
                return clazz.newInstance();
            }

            PropertyDescriptor pd = Misc.getPropertyDescriptor(parentInstance.getClass(), name);
            if (pd != null) {
                Class<?> type = pd.getPropertyType();

                Method setter = pd.getWriteMethod();
                // if it is a collection we create an instance of the subtype and add it to the collection
                // we return the subtype and not the collection
                if (Collection.class.isAssignableFrom(type)) {
                    type = Misc.genericClass(setter.getGenericParameterTypes()[0]);

                    return type.newInstance();
                } else {
                    return type.newInstance();
                }
            } else {
                throw new PersistenceException(parentInstance.getClass() + " does not have setter for " + name);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void apply(Object instance, String name, Object value) {
        try {
            PropertyDescriptor pd = Misc.getPropertyDescriptor(instance.getClass(), name);
            if (pd != null) {
                Class<?> type = pd.getPropertyType();

                Method setter = pd.getWriteMethod();
                // if it is a collection we create an instance of the subtype and add it to the collection
                // we return the subtype and not the collection
                if (Collection.class.isAssignableFrom(type)) {
                    Collection collection = (Collection) pd.getReadMethod().invoke(instance);
                    if (collection == null) {
                        collection = new LinkedHashSet<>();
                        setter.invoke(instance, collection);
                    }
                    collection.add(value);
                } else {
                    setter.invoke(instance, value);
                }
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean map(ResultSetWrapper rsw, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;
            for (MapColumn mapColumn : mapColumns) {

                PropertyDescriptor pd = Misc.getPropertyDescriptor(instance.getClass(), mapColumn.getAlias());
                if (pd != null) {
                    Class<?> type = pd.getPropertyType();

                    Object value = driver.fromDb(rsw, mapColumn.getColumnIndex(), type);
                    pd.getWriteMethod().invoke(instance, value);
                    touched |= value != null;
                }
            }

            return touched;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
