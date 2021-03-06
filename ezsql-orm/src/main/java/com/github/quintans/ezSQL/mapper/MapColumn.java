package com.github.quintans.ezSQL.mapper;

public class MapColumn {
    private int columnIndex;
    private String alias;
    private boolean key;

    public MapColumn(int columnIndex, String alias, boolean key) {
        this.columnIndex = columnIndex;
        this.alias = alias;
        this.key = key;
    }

    public int getIndex() {
        return columnIndex;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isKey() {
        return key;
    }

    @Override
    public String toString() {
        return columnIndex + ": " + alias;
    }
}
