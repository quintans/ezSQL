package com.github.quintans.ezSQL.transformers;

import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MapTable {
    /**
     * table alias
     */
    private String tableAlias;
    /**
     * association alias
     */
    private String associationAlias;
    private String name;
    private List<Object> keys = new ArrayList<>();
    /**
     * domain object. This will never be a collection.
     */
    private Object instance;
    private List<MapColumn> mapColumns = new ArrayList<>();
    private List<MapTable> mapTables = new ArrayList<>();

    public MapTable(String tableAlias, String name, String associationAlias) {
        this.tableAlias = tableAlias;
        this.associationAlias = associationAlias;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getAssociationAlias() {
        return associationAlias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public Object getInstance() {
        return instance;
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public void addColumnNode(MapColumn mapColumn) {
        if (!mapColumns.contains(mapColumn)) {
            this.mapColumns.add(mapColumn);
        }
    }

    public void reset() {
        keys = new ArrayList<>();
        instance = null;
        for (MapTable mapTable : mapTables) {
            mapTable.reset();
        }
    }

    public List<MapColumn> getMapColumns() {
        return mapColumns;
    }

    public void addTableNode(MapTable mapTable) {
        if (!mapTables.contains(mapTable)) {
            mapTables.add(mapTable);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapTable mapTable = (MapTable) o;
        return tableAlias.equals(mapTable.tableAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableAlias);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableAlias + "." + name + " {");
        for (MapColumn cn : mapColumns) {
            sb.append(" ").append(cn).append(";");
        }
        sb.append(" } " + associationAlias);
        return sb.toString();
    }

    private boolean create(ResultSetWrapper rsw, Object parentInstance, Mapper mapper) {
        // collect all values from the columns
        instance = mapper.createFrom(parentInstance, associationAlias);
        boolean finalize = false;
        for (MapColumn cn : mapColumns) {
            if (mapper.map(rsw, instance, cn) != null) {
                finalize = true;
            }
        }
        return finalize;
    }

    public void process(ResultSetWrapper rsw, Map<List<Object>, Object> domainCache, int offset, Object parentInstance, Mapper mapper) {
        List<Object> keyValues = null;
        boolean finalize = false;
        if (domainCache != null) {
            keyValues = grabKeyValues(rsw, offset);
            if (!keyValues.isEmpty() && !keys.equals(keyValues)) {
                reset();

                instance = domainCache.get(keyValues);
                if (instance == null) {
                    finalize = create(rsw, parentInstance, mapper);
                } else {
                    finalize = true;
                }

                if (finalize) {
                    keys = keyValues;
                } else {
                    instance = null;
                }

            }
        } else {
            finalize = create(rsw, parentInstance, mapper);
        }

        if (finalize) {
            if (parentInstance != null) {
                mapper.apply(parentInstance, associationAlias, instance);
            }
        }

        if (instance != null) {
            for (MapTable mapTable : mapTables) {
                mapTable.process(rsw, domainCache, offset, instance, mapper);
            }
        }
    }

    private List<Object> grabKeyValues(ResultSetWrapper rsw, int offset) {
        List<Object> keyValues = new ArrayList<>();
        keyValues.add(tableAlias);
        for (MapColumn cn : mapColumns) {
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
}
