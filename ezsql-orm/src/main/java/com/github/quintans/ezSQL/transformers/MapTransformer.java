package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.*;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class MapTransformer<T> implements IQueryRowTransformer<T> {
    private TableNode rootNode;
    private Map<String, TableNode> tableNodes;
    private Map<List<?>, Object> domainCache;

    private Query query;
    private Driver driver;
    private boolean reuse;
    private int offset;

    public MapTransformer(Query query, boolean reuse) {
        this.query = query;
        this.reuse = reuse;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public void setQuery(Query query) {
        this.query = query;
    }

    @Override
    public Collection<T> beforeAll(ResultSetWrapper rsw) {
        driver = query.getDb().getDriver();
        this.offset = query.getDb().getDriver().paginationColumnOffset(query);
        domainCache = new HashMap<>();

        buildTree();

        return new LinkedHashSet<T>();
    }

    private void buildTree() {
        // creates the tree
        tableNodes = new LinkedHashMap<>();

        Table table = query.getTable();
        rootNode = buildTreeAndCheckKeys(table, query.getTableAlias(), "");
        tableNodes.put(rootNode.getTableAlias(), rootNode);

        if (query.getJoins() != null) {
            for (Join join : query.getJoins()) {
                if (join.isFetch()) {
                    for (PathElement pe : join.getPathElements()) {
                        for (Association association : join.getAssociations()) {
                            String aliasTo = association.getAliasTo();

                            if (!tableNodes.containsKey(aliasTo)) {
                                TableNode tableNode = buildTreeAndCheckKeys(association.getTableTo(), aliasTo, association.getAlias());
                                tableNodes.put(aliasTo, tableNode);

                                TableNode parent = tableNodes.get(association.getAliasFrom());
                                parent.addTableNode(tableNode);
                            }
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
    private TableNode buildTreeAndCheckKeys(Table table, String tableAlias, String associationAlias) {
        TableNode tableNode = new TableNode(tableAlias, associationAlias);

        int tableKeys = 0;
        int queryKeys = 0;
        int index = 0;
        for (Function column : query.getColumns()) {
            index++; // column position starts at 1

            boolean isKey = false;
            if (column instanceof ColumnHolder) {
                ColumnHolder ch = (ColumnHolder) column;

                if (reuse && ch.getColumn().isKey() && ch.getTableAlias().equals(tableAlias) ) {
                    isKey = true;
                    queryKeys++;

                    for (Column<?> col : table.getColumns()) {
                        if (col.equals(ch.getColumn())) {
                            tableKeys++;
                            break;
                        }
                    }
                }

            }

            if (column.getPseudoTableAlias().equals(tableAlias)) {
                tableNode.addColumnNode(new ColumnNode(offset + index, column.getAlias(), isKey));
            }
        }

        if (reuse && tableKeys != queryKeys) {
            throw new PersistenceException("At least one Key column was not found for " + table.toString()
                    + ". When transforming to a object tree and reusing previous beans, ALL key columns must be declared in the select.");
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
        keyValues.add(tableNode.getTableAlias());
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
        if (keyValues.size() == 1) {
            keyValues.clear();
        }
        return keyValues;
    }

    @Override
    public void onTransformation(Collection<T> result, T object) {
        if (object != null && (!this.reuse || !result.contains(object))) {
            result.add(object);
        }
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

    protected <T> T fromDb(ResultSetWrapper rsw, int columnIndex, Class<T> type) throws SQLException {
        return driver.fromDb(rsw, columnIndex, type);
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
     * @param name           name of the parent property that we want to instantiate for
     * @return the new instance
     */
    public abstract Object instantiate(Object parentInstance, String name);

    /**
     * collecting the data from the database and put in the domain instance.
     * The value from a column is always put in an domain instance.
     *
     * @param rsw
     * @param instance
     * @param columnNode
     * @return
     */
    public abstract Object property(ResultSetWrapper rsw, Object instance, ColumnNode columnNode);

}
