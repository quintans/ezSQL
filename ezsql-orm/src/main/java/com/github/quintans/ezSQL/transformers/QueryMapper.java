package com.github.quintans.ezSQL.transformers;

import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.List;

public interface QueryMapper {
    /**
     * Method called to get domain instance when calling the <code>property()</code> method.
     * This method is only called when there is a need to create a new instance.
     * If the parent instance is null, it means we are asking for the root object to be instantiated.
     * Otherwise we are asking an instance of the type defined by the property defined in 'name' in the parent instance.
     * Override this method to provide your own implementation for a different domain object.
     * For example we might want to return Map<String, Object>. We could even return a builder instance.
     *
     * @param parentInstance parent instance
     * @param name           name of the parent property that we want to instantiate for
     * @return the created instance
     */
    Object createFrom(Object parentInstance, String name);

    /**
     * Establishes the relationship between the parent instance and the new instance.
     * It will be called only if there is parent instance.
     *
     * @param parentInstance
     * @param name name of the parent property that we want to set for
     * @param value the instance
     */
    void apply(Object parentInstance, String name, Object value);

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
