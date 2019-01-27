package com.github.quintans.ezSQL.transformers;

import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.List;

public interface Mapper {
    /**
     * Method called to get domain instance (POJO) when calling the <code>property()</code> method.
     * This method is only called when there is a need to create a new instance.
     * If the parent instance is null, it means we are asking for the root object to be instantiated.
     * Otherwise we are asking an instance of the type defined by the property defined in 'name' in the parent instance.
     * Override this method to provide your own implementation for a different domain object.
     * We defer the setting of the instance, by returning a lambda, because we may wish not to set the value.
     * e.g: in an outer join, all related fields came as null, therefore it is a empty entity
     *
     * @param parentInstance parent instance
     * @param name           name of the parent property that we want to instantiate for
     * @return lambda where we apply the the instance value to the parent instance
     */
    Object createFrom(Object parentInstance, String name);

    void apply(Object instance, String name, Object value);

    /**
     * collecting the data from the database and put in the domain instance.
     * The value from a column is always put in an domain instance.
     *
     * @param rsw resultset wrapper
     * @param instance domain instance to return
     * @param mapColumns column mapping info
     * @return true if the instance was populated with data
     */
    boolean map(ResultSetWrapper rsw, Object instance, List<MapColumn> mapColumns);
}
