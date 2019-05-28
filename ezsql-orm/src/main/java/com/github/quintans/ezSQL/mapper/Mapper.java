package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Join;
import com.github.quintans.ezSQL.dml.PathElement;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.mapper.MapColumn;
import com.github.quintans.ezSQL.mapper.MapTable;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.mapper.Row;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class Mapper<T> {

    private MapTable rootNode;
    private Map<List<Object>, Object> domainCache;

    private QueryDSL<?> query;
    private boolean reuse;
    private Class<T> rootClass;
    private QueryMapper mapper;

    public Mapper(QueryDSL query, boolean reuse, Class<T> rootClass, QueryMapper mapper) {
        this.query = query;
        this.reuse = reuse;
        this.rootClass = rootClass;
        this.mapper = mapper;
    }

    public Collection<T> init() {
        rootNode = buildTree();

        if (!this.query.isFlat() && reuse) {
            domainCache = new HashMap<>();
            return new LinkedHashSet<>();
        }

        return new ArrayList<>();
    }

    private MapTable buildTree() {
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
                mapTable.addColumnNode(new MapColumn(index, column.getAlias(), isKey));
            }
        }

        if (reuse && tableKeys != queryKeys) {
            throw new PersistenceException("At least one Key column was not found for " + table.toString()
                    + ". When transforming to a object tree and reusing previous beans, ALL key columns must be declared in the select.");
        }

        return mapTable;
    }

    @SuppressWarnings("unchecked")
    public T map(Row row) {
        // reuse
        if (domainCache == null) {
            rootNode.reset();
        }
        rootNode.process(row, domainCache, rootClass, null, this.mapper);

        return (T) rootNode.getInstance();
    }

    public void cleanUp(Collection<T> result) {
        // is null when there are no results
        if (rootNode != null) {
            rootNode.reset();
        }
    }

    public MapTable getRootNode() {
        return rootNode;
    }

    public Map<List<Object>, Object> getDomainCache() {
        return domainCache;
    }

    public QueryDSL<?> getQuery() {
        return query;
    }

    public boolean isReuse() {
        return reuse;
    }

    public Class<T> getRootClass() {
        return rootClass;
    }

    public QueryMapper getMapper() {
        return mapper;
    }
}
