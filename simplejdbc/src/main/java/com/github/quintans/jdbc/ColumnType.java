package com.github.quintans.jdbc;

public class ColumnType {
    /**
     * Column name
     */
    private String column;
    /**
     * SQL Type
     */
    private Class<?> type;

    public ColumnType(String column, Class<?> type) {
        this.column = column;
        this.type = type;
    }

    public String getColumn() {
        return column;
    }

    public Class<?> getType() {
        return type;
    }
}
