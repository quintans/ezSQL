package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.*;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.ezSQL.toolkit.utils.Strings;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MapTransformer<T> implements IRowTransformer<T> {
    private TableNode rootNode;
    private Map<String, TableNode> tableNodes;
    private Map<List<?>, Object> domainCache;

    private Query query;
    private Driver driver;
    private Class<T> clazz;
    private boolean reuse;
    private int offset;

    public MapTransformer(Query query, Class<T> clazz, boolean reuse) {
        this.query = query;
        driver = query.getDb().getDriver();
        this.clazz = clazz;
        this.reuse = reuse;
        this.offset = query.getDb().getDriver().paginationColumnOffset(query);
    }

    @Override
    public Collection<T> beforeAll(ResultSetWrapper rsw) {
        domainCache = new HashMap<>();

        buildTree();

        return new LinkedHashSet<T>();
    }

    private void buildTree() {
        // creates the tree
        tableNodes = new LinkedHashMap<>();

        Table table = query.getTable();
        rootNode = buildTreeAndCheckKeys(table);
        tableNodes.put(rootNode.getAlias(), rootNode);

        if (query.getJoins() != null) {
            for (Join join : query.getJoins()) {
                if (join.isFetch()) {
                    for (PathElement pe : join.getPathElements()) {
                        final Table tableTo = pe.getBase().getTableTo();
                        if (!tableNodes.containsKey(tableTo.getAlias())) {
                            TableNode tableNode = buildTreeAndCheckKeys(tableTo);
                            String fromAlias = pe.getBase().getTableFrom().getAlias();
                            TableNode parent = tableNodes.get(fromAlias);
                            parent.addTableNode(tableNode);
                        }
                    }
                }
            }
        }
    }

    /*
     * When reusing beans, the transformation needs all key columns defined.
     * A exception is thrown if there is NO key column.
     */
    private TableNode buildTreeAndCheckKeys(Table table) {
        String alias = table.getAlias();
        TableNode tableNode = new TableNode(alias);

        int index = 0;
        for (Column<?> col : table.getColumns()) {
            index++; // column position starts at 1
            boolean foundKey = false;
            for (Function column : query.getColumns()) {
                if (column instanceof ColumnHolder) {
                    ColumnHolder ch = (ColumnHolder) column;
                    if (col.equals(ch.getColumn())) {
                        if (reuse && col.isKey()) {
                            foundKey = true;
                            break;
                        }
                        tableNode.addColumnNode(new ColumnNode(offset + index, column.getAlias(), col.isKey()));
                    }
                }
            }
            if (reuse && !foundKey) {
                throw new PersistenceException("At least one Key column was not found for " + table.toString()
                        + ". When transforming to a object tree and reusing previous beans, ALL key columns must be declared in the select.");
            }
        }

        return tableNode;
    }

    @Override
    public T transform(ResultSetWrapper rsw) throws SQLException {
        rootNode.reset();
        for (TableNode tableNode : tableNodes.values()) {
            List<Object> keyValues = null;
            if (reuse) {
                keyValues = grabKeyValues(rsw, tableNode);
                if (!keyValues.isEmpty()) {
                    Object instance = domainCache.get(keyValues);
                    if (instance != null) {
                        tableNode.setInstance(instance);
                    }
                }
            }

            mapper(rsw, tableNode);

            if (reuse && !keyValues.isEmpty()) {
                domainCache.put(keyValues, tableNode.getInstance());
            }
        }
        return (T) rootNode.getInstance();
    }

    private List<Object> grabKeyValues(ResultSetWrapper rsw, TableNode tableNode) {
        List<Object> keyValues = new ArrayList<>();
        for (ColumnNode cn : tableNode.getColumnNodes()) {
            if (cn.isKey()) {
                try {
                    ResultSet rs = rsw.getResultSet();
                    Object value = rs.getObject(cn.getColumnIndex() + offset);
                    if (!rs.wasNull()) {
                        keyValues.add(value);
                    }
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
            }
        }
        return keyValues;
    }

    @Override
    public void onTransformation(Collection<T> result, T object) {

    }

    @Override
    public void afterAll(Collection<T> result) {

    }

    private Object mapper(ResultSetWrapper rsw, TableNode tableNode) {
        Object instance = tableNode.getInstanceIfAbsent(this::instantiate);

        try {
            for (ColumnNode cn : tableNode.getColumnNodes()) {
                property(rsw, instance, cn);
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
        return instance;
    }

    // the following methods should be overridden if we want to implement another domain builder

    /**
     * Method called to get domain instance (POJO) when calling the <code>property()</code> method.
     * This method is only called when there is a need to create a new instance.
     * If the parent instance is null, it means we are asking for the root object to be instantiated.
     * Otherwise we are asking an instance of the type defined by the property defined in 'name' in the parent instance.
     * Override this method to provide your own implementation for a different domain object.
     *
     * @param parentInstance parent instance
     * @param name name of the parent property that we want to instantiate for
     * @return the new instance
     */
    public Object instantiate(Object parentInstance, String name) {
        try {
            // handling root table
            if (parentInstance == null) {
                return clazz.newInstance();
            }

            String suffix = Strings.capitalizeFirst(name);
            Method setter = parentInstance.getClass().getMethod("set" + suffix);
            if (setter != null) {
                Object instance = null;
                Class<?>[] types = setter.getParameterTypes();
                Class<?> type = types[0];
                // if it is a collection we create an instance of the subtype and add it to the collection
                // we return the subtype and not the collection
                if (Collection.class.isAssignableFrom(type)) {
                    // are asking for a member that is a collection
                    type = Misc.genericClass(setter.getGenericParameterTypes()[0]);
                    Method getter = parentInstance.getClass().getMethod("get" + suffix);
                    Collection collection = (Collection) getter.invoke(parentInstance);
                    if (collection != null) {
                        collection = new LinkedHashSet<>();
                        setter.invoke(parentInstance, collection);
                    }
                    instance = type.newInstance();
                    collection.add(type.newInstance());
                } else {
                    instance = type.newInstance();
                }

                return instance;
            } else {
                throw new PersistenceException(parentInstance.getClass() + " does not have setter for " + name);
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * collecting the data from the database and put in the domain instance.
     * The value from a column is always put in an domain instance.
     *
     * @param rsw
     * @param instance
     * @param columnNode
     * @return
     */
    public Object property(ResultSetWrapper rsw, Object instance, ColumnNode columnNode) {
        try {
            Object value = null;
            String suffix = Strings.capitalizeFirst(columnNode.getAlias());
            Method setter = instance.getClass().getMethod("set" + suffix);
            if (setter != null) {
                if (!setter.isAccessible()) {
                    setter.setAccessible(true);
                }
                Class<?>[] types = setter.getParameterTypes();
                Class<?> type = types[0];

                value = driver.fromDb(rsw, columnNode.getColumnIndex(), type);
                setter.invoke(instance, value);
            }
            return value;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
