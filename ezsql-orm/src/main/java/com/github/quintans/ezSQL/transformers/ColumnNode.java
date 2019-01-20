package com.github.quintans.ezSQL.transformers;

public class ColumnNode {
    private int columnIndex;
    private String alias;
    private boolean key;

    public ColumnNode(int columnIndex, String alias, boolean key) {
        this.columnIndex = columnIndex;
        this.alias = alias;
        this.key = key;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isKey() {
        return key;
    }
}
