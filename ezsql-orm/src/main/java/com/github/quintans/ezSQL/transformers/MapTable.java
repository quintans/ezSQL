package com.github.quintans.ezSQL.transformers;

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

    private boolean create(Record record, Object parentInstance, QueryMapper mapper) {
        // collect all values from the columns
        instance = mapper.createFrom(parentInstance, associationAlias);
        boolean finalize = false;
        if (mapper.map(record, instance, mapColumns)) {
            finalize = true;
        }
        return finalize;
    }

    public void process(Record record, Map<List<Object>, Object> domainCache, Object parentInstance, QueryMapper mapper) {
        List<Object> keyValues;
        boolean finalize = false;
        if (domainCache != null) {
            keyValues = grabKeyValues(record);
            if (!keyValues.isEmpty() && !keys.equals(keyValues)) {
                reset();

                instance = domainCache.get(keyValues);
                if (instance == null) {
                    finalize = create(record, parentInstance, mapper);
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
            finalize = create(record, parentInstance, mapper);
        }

        if (finalize) {
            if (parentInstance != null) {
                mapper.link(parentInstance, associationAlias, instance);
            }
        }

        if (instance != null) {
            for (MapTable mapTable : mapTables) {
                mapTable.process(record, domainCache, instance, mapper);
            }
        }
    }

    private List<Object> grabKeyValues(Record record) {
        List<Object> keyValues = new ArrayList<>();
        keyValues.add(tableAlias);
        for (MapColumn mc : mapColumns) {
            if (mc.isKey()) {
                Object value = record.getObject(mc.getIndex());
                if (value != null) {
                    keyValues.add(value);
                }
            }
        }
        if (keyValues.size() == 1) {
            keyValues.clear();
        }
        return keyValues;
    }
}
