package com.github.quintans.jdbc.transformers;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.github.quintans.jdbc.exceptions.PersistenceException;

public class ResultSetWrapper {
    private ResultSet rs;
    private int[] columnTypes;

    public ResultSetWrapper(ResultSet rs) {
        this.rs = rs;
    }

    public ResultSet getResultSet() {
        return rs;
    }

    public int[] getColumnTypes() {
        if (columnTypes == null) {
            populateSqlTypes();
        }
        return columnTypes;
    }

    public int getSqlType(int columnIndex) {
        if (columnTypes == null) {
            populateSqlTypes();
        }
        return columnTypes[columnIndex - 1];
    }

    private void populateSqlTypes() {
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            columnTypes = new int[cols];
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }
        } catch (SQLException e) {
            throw new PersistenceException("Unable to get ResultSetMetaData ", e);
        }
    }
}
