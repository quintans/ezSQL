package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.*;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.SQLException;
import java.util.*;

public class MapTransformer<T> implements IRowTransformer<T> {

    private MapTable rootNode;
    private Map<List<Object>, Object> domainCache;

    private Query query;
    private int offset;
    private boolean reuse;
    private QueryMapper mapper;

    public MapTransformer(Query query, boolean reuse, QueryMapper mapper) {
        this.query = query;
        this.reuse = reuse;
        this.mapper = mapper;
    }

    @Override
    public Collection<T> beforeAll(ResultSetWrapper rsw) {
        this.offset = query.getDb().getDriver().paginationColumnOffset(query);
        if (!this.query.isFlat() && reuse) {
            domainCache = new HashMap<>();
        }

        rootNode = buildTree();

        return this.domainCache != null ? new LinkedHashSet<>() : new ArrayList<T>();
    }

    protected MapTable buildTree() {
        // creates the tree
        Map<String, MapTable> tableNodes = new HashMap<>();


        Table table = query.getTable();
        MapTable rootNode = buildTreeAndCheckKeys(table, query.getTableAlias(), "");
        tableNodes.put(rootNode.getTableAlias(), rootNode);

        if (query.getJoins() != null) {
            for (Join join : query.getJoins()) {
                if (join.isFetch()) {
                    for (PathElement pe : join.getPathElements()) {
                        for (Association fk : join.getAssociations()) {
                            String aliasTo = fk.isMany2Many() ? fk.getToM2M().getAliasTo() : fk.getAliasTo();

                            if (!tableNodes.containsKey(aliasTo)) {
                                Table tableTo = fk.isMany2Many() ? fk.getToM2M().getTableTo() : fk.getTableTo();
                                MapTable mapTable = buildTreeAndCheckKeys(tableTo, aliasTo, fk.getAlias());
                                tableNodes.put(aliasTo, mapTable);

                                String aliasFrom = fk.isMany2Many() ? fk.getFromM2M().getAliasFrom() : fk.getAliasFrom();
                                MapTable parent = tableNodes.get(aliasFrom);
                                parent.addTableNode(mapTable);
                            }
                        }
                    }
                }
            }
        }

        return rootNode;
    }

    /*
     * When reusing beans, the transformation needs all key columns defined.
     * A exception is thrown if there is NO key column.
     */
    private MapTable buildTreeAndCheckKeys(Table table, String tableAlias, String associationAlias) {
        MapTable mapTable = new MapTable(tableAlias, table.getName(), associationAlias);

        int tableKeys = 0;
        int queryKeys = 0;
        int index = 0;
        for (Function column : query.getColumns()) {
            index++; // column position starts at 1

            boolean isKey = false;
            if (reuse) {
                if (column instanceof ColumnHolder) {
                    ColumnHolder ch = (ColumnHolder) column;

                    if (ch.getColumn().isKey() && ch.getTableAlias().equals(tableAlias)) {
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
            }

            // overrides the column table alias when the query result is flat
            String pseudoAlias;
            if (query.isFlat()) {
                pseudoAlias = query.getTableAlias();
            } else {
                pseudoAlias = column.getPseudoTableAlias();
            }

            if (tableAlias.equals(pseudoAlias)) {
                mapTable.addColumnNode(new MapColumn(offset + index, column.getAlias(), isKey));
            }
        }

        if (reuse && tableKeys != queryKeys) {
            throw new PersistenceException("At least one Key column was not found for " + table.toString()
                    + ". When transforming to a object tree and reusing previous beans, ALL key columns must be declared in the select.");
        }

        return mapTable;
    }

    @Override
    public T transform(ResultSetWrapper rsw) throws SQLException {
        // reuse
        if (domainCache == null) {
            rootNode.reset();
        }
        rootNode.process(rsw, domainCache, offset, null, this.mapper);

        return (T) rootNode.getInstance();
    }


    @Override
    public void onTransformation(Collection<T> result, T object) {
        if (object != null) {
            result.add(object);
        }
    }

    @Override
    public void afterAll(Collection<T> result) {
        // is null when there are no results
        if (rootNode != null) {
            rootNode.reset();
        }
    }

}
